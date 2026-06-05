import io
import json
import os
import queue
import threading
import zipfile
import uuid
from collections import defaultdict
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import Flask, render_template, request, jsonify, Response, send_file, stream_with_context

app = Flask(__name__)

# ── Config ───────────────────────────────────────────────
B2_KEY_ID      = os.environ.get("B2_KEY_ID",      "")
B2_APP_KEY     = os.environ.get("B2_APP_KEY",     "")
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME", "renombot-sis")
B2_ENDPOINT    = os.environ.get("B2_ENDPOINT",    "")
MAX_UPLOAD_MB  = int(os.environ.get("MAX_UPLOAD_MB", "500"))
MAX_JOBS       = int(os.environ.get("MAX_JOBS", "5"))

# ── Estado por sesión ────────────────────────────────────
jobs_lock = threading.Lock()
jobs: dict = {}

def new_job() -> str:
    job_id = uuid.uuid4().hex
    with jobs_lock:
        # Purgar jobs terminados si hay demasiados
        if len(jobs) > MAX_JOBS * 2:
            done = [jid for jid, j in jobs.items() if not j["running"]]
            for jid in done:
                del jobs[jid]
        jobs[job_id] = {
            "log_queue": queue.Queue(),
            "running": False,
            "zip_bytes": None,
        }
    return job_id

def get_job(job_id: str):
    with jobs_lock:
        return jobs.get(job_id)

def job_log(job_id: str, msg: str, tipo: str = "info"):
    j = get_job(job_id)
    if j:
        ts = datetime.now().strftime("%H:%M:%S")
        j["log_queue"].put({"ts": ts, "msg": msg, "tipo": tipo})

# ── B2 client ────────────────────────────────────────────
def get_b2_client():
    if not all([B2_KEY_ID, B2_APP_KEY, B2_ENDPOINT]):
        return None
    return boto3.client(
        "s3",
        endpoint_url=f"https://{B2_ENDPOINT}",
        aws_access_key_id=B2_KEY_ID,
        aws_secret_access_key=B2_APP_KEY,
        config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=30),
    )

# ── Helpers ──────────────────────────────────────────────
def folder_name_from(name: str) -> str:
    stem = name
    for ext in ('.zip', '.ZIP'):
        if stem.endswith(ext):
            stem = stem[:-len(ext)]
            break
    parts = stem.split('-')
    return parts[-1] if len(parts) > 1 else stem

def is_junk_segment(s: str) -> bool:
    if not s:
        return True
    low = s.lower()
    if low.startswith('descargamasiva-'):
        return True
    if s.isdigit() and len(s) >= 8:
        return True
    if low in ('downloads', 'descargas', 'temp', 'tmp', 'desktop', 'documents'):
        return True
    return False

def unpack_zip_bytes(data: bytes, zip_filename: str, job_id: str) -> list:
    dest_folder = folder_name_from(zip_filename)
    results = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            bad = zf.testzip()
            if bad:
                job_log(job_id, f"  ⚠ Sector corrupto: {bad}", "warn")
            for info in zf.infolist():
                if info.is_dir():
                    continue
                raw = info.filename.replace('\\', '/')
                parts = [p for p in raw.split('/') if p]
                if not parts:
                    continue
                file_parts = parts[1:] if len(parts) > 1 else parts
                file_parts = [p for p in file_parts if not is_junk_segment(p)]
                if not file_parts:
                    continue
                final = dest_folder + '/' + '/'.join(file_parts)
                results.append((final, zf.read(info)))
        job_log(job_id, f"  ZIP {zip_filename} → {dest_folder}/ ({len(results)} archivos)", "ok")
    except zipfile.BadZipFile:
        job_log(job_id, f"  ERROR: {zip_filename} no es un ZIP válido", "error")
    except Exception as e:
        job_log(job_id, f"  ERROR al abrir {zip_filename}: {e}", "error")
    return results

def collect(uploaded_files, job_id: str):
    items = []
    warnings = []
    for key in uploaded_files:
        if not key.startswith('file_'):
            continue
        idx   = key[5:]
        fs    = uploaded_files[key]
        data  = fs.read()
        size_mb = len(data) / (1024 * 1024)
        if size_mb > MAX_UPLOAD_MB:
            warnings.append(f"{fs.filename}: excede {MAX_UPLOAD_MB} MB ({size_mb:.1f} MB)")
            continue
        fs_name    = (fs.filename or '').strip('"').strip()
        path_raw   = request.form.get(f'path_{idx}', fs_name).strip('"').strip()
        path_norm  = path_raw.replace('\\', '/')
        path_parts = [p for p in path_norm.split('/') if p]
        last_seg   = path_parts[-1] if path_parts else fs_name

        def ends_zip(s): return s.lower().endswith('.zip')
        is_zip = ends_zip(fs_name) or ends_zip(last_seg)

        if is_zip:
            zip_name = fs_name if ends_zip(fs_name) else last_seg
            unpacked = unpack_zip_bytes(data, zip_name, job_id)
            if not unpacked:
                warnings.append(f"{zip_name}: sin archivos válidos")
            items.extend(unpacked)
        else:
            clean = [p for p in path_parts if not is_junk_segment(p)]
            if not clean:
                continue
            root = clean[0]
            if '-' in root:
                clean[0] = folder_name_from(root)
            items.append(('/'.join(clean), data))
    return items, warnings

def run_bot(job_id: str, items: list):
    j = get_job(job_id)
    if not j:
        return
    j["running"]   = True
    j["zip_bytes"] = None

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_log(job_id, "─────────────────────────────────", "head")
    job_log(job_id, f"Inicio: {ts}", "head")
    job_log(job_id, f"Archivos totales: {len(items)}", "info")
    job_log(job_id, "─────────────────────────────────", "head")

    if not items:
        job_log(job_id, "Sin archivos para procesar.", "warn")
        j["running"] = False
        return

    root_count = defaultdict(int)
    for path, _ in items:
        root_count[path.split('/')[0]] += 1

    used = set()
    rename_map = {}
    for root in sorted(root_count):
        new = root
        if new in used:
            c = 1
            while f"{new}-{c}" in used:
                c += 1
            new = f"{new}-{c}"
            job_log(job_id, f"  Duplicado: {root} → {new}", "warn")
        used.add(new)
        rename_map[root] = new

    buf      = io.BytesIO()
    ok       = 0
    err_list = []
    ok_roots = defaultdict(int)

    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zout:
        for path, data in sorted(items, key=lambda x: x[0]):
            root     = path.split('/')[0]
            new_root = rename_map.get(root, root)
            new_path = new_root + path[len(root):]
            try:
                zout.writestr(new_path, data)
                ok_roots[new_root] += 1
                ok += 1
            except Exception as e:
                err_list.append((new_path, str(e)))

    for root_name, count in sorted(ok_roots.items()):
        job_log(job_id, f"  ✓ {root_name}/ ({count} archivo{'s' if count > 1 else ''})", "ok")
    for path, err in err_list:
        job_log(job_id, f"  ERR {path}: {err}", "error")

    buf.seek(0)
    j["zip_bytes"] = buf.read()

    # Subir a B2
    client = get_b2_client()
    if client:
        zip_name = f"Renombrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id[:6]}.zip"
        try:
            client.put_object(
                Bucket=B2_BUCKET_NAME,
                Key=zip_name,
                Body=io.BytesIO(j["zip_bytes"]),
                ContentType='application/zip',
            )
            job_log(job_id, f"  ↑ Subido a Backblaze: {zip_name}", "ok")
        except ClientError as e:
            job_log(job_id, f"  ✗ Backblaze error: {e}", "error")

    job_log(job_id, "─────────────────────────────────", "head")
    job_log(job_id, f"Listo: {ok} archivos en {len(ok_roots)} carpetas", "head")
    job_log(job_id, "Proceso completado.", "ok")
    j["running"] = False

# ── B2 background clear ──────────────────────────────────
# Se ejecuta en thread para no bloquear el request (Railway timeout = 30s)
b2_clear_status = {"running": False, "last_result": None}
b2_clear_lock   = threading.Lock()

def _do_clear_b2():
    with b2_clear_lock:
        b2_clear_status["running"] = True
        b2_clear_status["last_result"] = None
    client = get_b2_client()
    deleted = []
    errors  = []
    if client:
        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    try:
                        client.delete_object(Bucket=B2_BUCKET_NAME, Key=key)
                        deleted.append(key)
                    except ClientError as e:
                        errors.append(f"{key}: {e}")
        except ClientError as e:
            errors.append(str(e))
    with b2_clear_lock:
        b2_clear_status["running"]     = False
        b2_clear_status["last_result"] = {
            "deleted_count": len(deleted),
            "errors": errors,
            "ok": len(errors) == 0,
        }

# ── Rutas Flask ──────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route('/favicon.ico')
def favicon():
    return send_file('static/favicon.ico', mimetype='image/x-icon')

@app.route("/job/new", methods=["POST"])
def job_new():
    with jobs_lock:
        active = sum(1 for j in jobs.values() if j["running"])
    if active >= MAX_JOBS:
        return jsonify({"error": f"Servidor ocupado ({active}/{MAX_JOBS} trabajos activos). Intenta en un momento."}), 503
    return jsonify({"job_id": new_job()})

@app.route("/preview", methods=["POST"])
def preview():
    tmp_id = new_job()
    items, warnings = collect(request.files, tmp_id)
    # Limpiar job temporal
    with jobs_lock:
        jobs.pop(tmp_id, None)
    roots = {}
    for path, _ in items:
        root = path.split('/')[0]
        roots[root] = roots.get(root, 0) + 1
    return jsonify({
        "items": [{"nombre": r, "archivos": c} for r, c in sorted(roots.items())],
        "warnings": warnings,
    })

@app.route("/run", methods=["POST"])
def run():
    job_id = request.form.get("job_id", "")
    j = get_job(job_id)
    if not j:
        return jsonify({"error": "Sesión inválida. Recarga la página."}), 400
    if j["running"]:
        return jsonify({"error": "Ya hay un proceso activo en esta sesión"}), 400
    items, warnings = collect(request.files, job_id)
    if not items:
        msg = "No se recibieron archivos válidos"
        if warnings:
            msg += ": " + "; ".join(warnings)
        return jsonify({"error": msg}), 400
    while not j["log_queue"].empty():
        j["log_queue"].get()
    for w in warnings:
        job_log(job_id, f"⚠ {w}", "warn")
    threading.Thread(target=run_bot, args=(job_id, items), daemon=True).start()
    return jsonify({"ok": True, "warnings": warnings})

@app.route("/download/<job_id>")
def download(job_id):
    j = get_job(job_id)
    if not j or not j["zip_bytes"]:
        return "No hay ZIP disponible", 404
    return send_file(
        io.BytesIO(j["zip_bytes"]),
        as_attachment=True,
        download_name="Renombrado.zip",
        mimetype="application/zip"
    )

@app.route("/stream/<job_id>")
def stream(job_id):
    j = get_job(job_id)
    if not j:
        return "Job no encontrado", 404
    def generate():
        while True:
            try:
                entry = j["log_queue"].get(timeout=30)
                yield f"data: {json.dumps(entry)}\n\n"
            except queue.Empty:
                yield 'data: {"ping":true}\n\n'
    return Response(stream_with_context(generate()), mimetype="text/event-stream")

@app.route("/status/<job_id>")
def status(job_id):
    j = get_job(job_id)
    if not j:
        return jsonify({"error": "Job no encontrado"}), 404
    with jobs_lock:
        active = sum(1 for jj in jobs.values() if jj["running"])
    return jsonify({
        "running":       j["running"],
        "zip_ready":     j["zip_bytes"] is not None,
        "b2_configured": bool(B2_KEY_ID and B2_APP_KEY and B2_ENDPOINT),
        "active_jobs":   active,
        "max_jobs":      MAX_JOBS,
    })

@app.route("/b2-status")
def b2_status():
    client = get_b2_client()
    if not client:
        return jsonify({"configured": False})
    try:
        total_size = 0
        files = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
            for obj in page.get("Contents", []):
                total_size += obj["Size"]
                files.append({
                    "name":     obj["Key"],
                    "size":     obj["Size"],
                    "modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                })
        return jsonify({
            "configured":  True,
            "file_count":  len(files),
            "total_bytes": total_size,
            "total_mb":    round(total_size / 1024 / 1024, 2),
            "files":       files,
        })
    except ClientError as e:
        return jsonify({"configured": True, "error": str(e)}), 500

@app.route("/clear-b2", methods=["POST"])
def clear_b2():
    """
    Lanza la limpieza en background y responde inmediatamente.
    Así no choca con el timeout de 30s de Railway.
    """
    with b2_clear_lock:
        if b2_clear_status["running"]:
            return jsonify({"status": "already_running", "msg": "Ya hay una limpieza en curso"}), 202

    threading.Thread(target=_do_clear_b2, daemon=True).start()
    return jsonify({"status": "started", "msg": "Limpieza iniciada en segundo plano"})

@app.route("/clear-b2/status")
def clear_b2_status():
    """El frontend hace polling aquí para saber si terminó la limpieza."""
    with b2_clear_lock:
        return jsonify({
            "running":     b2_clear_status["running"],
            "last_result": b2_clear_status["last_result"],
        })

if __name__ == "__main__":
    print("\nRenomBot SIS — http://localhost:5000\n")
    app.run(debug=False, threaded=True, host="0.0.0.0",
            port=int(os.environ.get("PORT", 5000)))
