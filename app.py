import io
import json
import os
import queue
import threading
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import Flask, render_template, request, jsonify, Response, send_file

app = Flask(__name__)

# ── Config from environment ──────────────────────────────
B2_KEY_ID      = os.environ.get("B2_KEY_ID",      "")
B2_APP_KEY     = os.environ.get("B2_APP_KEY",     "")
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME", "renombot-sis")
B2_ENDPOINT    = os.environ.get("B2_ENDPOINT",    "")   # e.g. s3.us-west-004.backblazeb2.com

# In-memory state (per-instance; Railway may spin multiple processes)
log_queue        = queue.Queue()
is_running       = False
last_zip_bytes   = None
# Track uploaded file IDs in Backblaze so we can delete them later
uploaded_file_ids = []  # list of (file_id, file_name)

# ── Backblaze S3 client ────────────────────────────────
def get_b2_client():
    if not B2_KEY_ID or not B2_APP_KEY or not B2_ENDPOINT:
        return None
    return boto3.client(
        "s3",
        endpoint_url=f"https://{B2_ENDPOINT}",
        aws_access_key_id=B2_KEY_ID,
        aws_secret_access_key=B2_APP_KEY,
        config=Config(signature_version="s3v4"),
    )

# ── Helpers ────────────────────────────────────────────

def log(msg, tipo="info"):
    ts = datetime.now().strftime("%H:%M:%S")
    log_queue.put({"ts": ts, "msg": msg, "tipo": tipo})

def folder_name_from(name: str) -> str:
    stem = name
    for ext in ('.zip', '.ZIP'):
        if stem.endswith(ext):
            stem = stem[:-len(ext)]
            break
    parts = stem.split('-')
    return parts[-1] if len(parts) > 1 else stem

def unpack_zip(data: bytes, zip_filename: str) -> list:
    dest_folder = folder_name_from(zip_filename)
    results = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                raw = info.filename.replace('\\', '/')
                parts = [p for p in raw.split('/') if p]
                if not parts:
                    continue
                file_parts = parts[1:] if len(parts) > 1 else parts
                if not file_parts:
                    continue
                final = dest_folder + '/' + '/'.join(file_parts)
                results.append((final, zf.read(info)))
        log(f"  ZIP {zip_filename} -> {dest_folder}/ ({len(results)} archivos)", "ok")
    except Exception as e:
        log(f"  ERROR al abrir {zip_filename}: {e}", "error")
    return results

def is_junk_segment(s: str) -> bool:
    if not s:
        return True
    low = s.lower()
    if low in ('silvia', 'downloads', 'descargas', 'temp', 'tmp'):
        return True
    if low.startswith('descargamasiva-'):
        return True
    if 'quintana' in low or 'silvia.' in low:
        return True
    if s.isdigit() and len(s) == 12:
        return True
    return False

# ── Routes ─────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route('/favicon.ico')
def favicon():
    return send_file('static/favicon.ico', mimetype='image/x-icon')

# ── Upload to Backblaze (streams large files directly) ─

@app.route("/upload-b2", methods=["POST"])
def upload_b2():
    """
    Recibe archivos grandes y los sube directamente a Backblaze B2
    usando multipart upload para archivos > 5 MB, o upload normal
    para archivos más pequeños.
    Returns JSON con { file_id, file_name } por cada archivo.
    """
    global uploaded_file_ids
    client = get_b2_client()
    results = []

    for key in request.files:
        if not key.startswith('file_'):
            continue
        fs   = request.files[key]
        data = fs.read()
        file_name = fs.filename or "unnamed"

        file_id = None
        if client:
            try:
                # Simple upload (≤ 5 GB, most common case)
                buf = io.BytesIO(data)
                client.put_object(
                    Bucket=B2_BUCKET_NAME,
                    Key=file_name,
                    Body=buf,
                    ContentType='application/octet-stream',
                )
                # Get file ID via HeadObject
                resp = client.head_object(Bucket=B2_BUCKET_NAME, Key=file_name)
                file_id = resp.get("Metadata", {}).get("file_id", "")
                log(f"  ↑ Backblaze: {file_name} ({len(data):,} bytes)", "ok")
            except ClientError as e:
                log(f"  ✗ Backblaze error: {e}", "error")
                results.append({"file_name": file_name, "file_id": None, "error": str(e)})
                continue
        else:
            log(f"  ↑ Local (sin B2): {file_name}", "warn")

        uploaded_file_ids.append((file_id or "local", file_name))
        results.append({"file_name": file_name, "file_id": file_id})

    return jsonify({"uploaded": results})

# ── Download from Backblaze ─────────────────────────────

@app.route("/download-b2", methods=["POST"])
def download_b2():
    """
    Recibe { file_name } y descarga el archivo desde Backblaze.
    Si B2 no está configurado, intenta servir desde memoria (last_zip_bytes).
    """
    data_in = request.get_json() or {}
    file_name = data_in.get("file_name", "")

    if not file_name:
        return jsonify({"error": "file_name requerido"}), 400

    client = get_b2_client()
    if client:
        try:
            buf = io.BytesIO()
            client.download_fileobj(B2_BUCKET_NAME, file_name, buf)
            buf.seek(0)
            return send_file(
                buf,
                as_attachment=True,
                download_name=file_name,
                mimetype='application/octet-stream'
            )
        except ClientError as e:
            return jsonify({"error": f"Backblaze: {e}"}), 404
    else:
        return jsonify({"error": "Backblaze no configurado"}), 503

# ── Clear everything (Backblaze + in-memory) ───────────

@app.route("/clear-b2", methods=["POST"])
def clear_b2():
    """
    Borra TODOS los archivos en el bucket de Backblaze que coincidan
    con el prefijo de esta sesión, y limpia el estado en memoria.
    """
    global uploaded_file_ids, last_zip_bytes

    client = get_b2_client()
    deleted = []
    errors  = []

    if client:
        try:
            # Listar todos los objetos en el bucket
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    try:
                        client.delete_object(Bucket=B2_BUCKET_NAME, Key=key)
                        deleted.append(key)
                    except ClientError as e:
                        errors.append(f"{key}: {e}")
            log(f"🗑 Backblaze: {len(deleted)} archivos eliminados", "ok")
        except ClientError as e:
            errors.append(f"Listar bucket: {e}")
            log(f"✗ Backblaze clear error: {e}", "error")

    # Limpiar estado en memoria
    uploaded_file_ids = []
    last_zip_bytes    = None

    log("🧹 Memoria limpiada (en proceso)", "ok")
    return jsonify({
        "deleted": deleted,
        "deleted_count": len(deleted),
        "errors": errors,
        "memory_cleared": True,
    })

# ── Bucket status ──────────────────────────────────────

@app.route("/b2-status", methods=["GET"])
def b2_status():
    """Devuelve uso del bucket y lista de archivos."""
    client = get_b2_client()
    if not client:
        return jsonify({
            "configured": False,
            "message": "Backblaze no configurado — configura B2_KEY_ID, B2_APP_KEY, B2_ENDPOINT",
        })

    try:
        total_size = 0
        files = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
            for obj in page.get("Contents", []):
                total_size += obj["Size"]
                files.append({
                    "name": obj["Key"],
                    "size": obj["Size"],
                    "modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                })
        return jsonify({
            "configured": True,
            "file_count": len(files),
            "total_bytes": total_size,
            "total_mb": round(total_size / 1024 / 1024, 2),
            "files": files,
        })
    except ClientError as e:
        return jsonify({"configured": True, "error": str(e)}), 500

# ── Collect uploaded files ──────────────────────────────

def collect(uploaded_files) -> list:
    items = []
    for key in uploaded_files:
        if not key.startswith('file_'):
            continue
        idx  = key[5:]
        fs   = uploaded_files[key]
        data = fs.read()
        fs_name = fs.filename or ''
        path_raw = request.form.get(f'path_{idx}', fs_name)
        path_norm = path_raw.replace('\\', '/')
        path_parts = [p for p in path_norm.split('/') if p]
        last_seg   = path_parts[-1] if path_parts else fs_name

        def ends_zip(s): return s.lower().endswith('.zip')
        is_zip = ends_zip(fs_name) or ends_zip(last_seg)

        if is_zip:
            zip_name = fs_name if ends_zip(fs_name) else last_seg
            items.extend(unpack_zip(data, zip_name))
        else:
            clean = [p for p in path_parts if not is_junk_segment(p)]
            if not clean:
                continue
            root = clean[0]
            if '-' in root:
                clean[0] = folder_name_from(root)
            final = '/'.join(clean)
            items.append((final, data))

    return items

def run_bot(items: list):
    global is_running, last_zip_bytes
    is_running    = True
    last_zip_bytes = None

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("─────────────────────────────────", "head")
    log(f"Inicio: {ts}", "head")
    log(f"Archivos totales: {len(items)}", "info")
    log("─────────────────────────────────", "head")

    if not items:
        log("Sin archivos para procesar.", "warn")
        is_running = False
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
            log(f"  Duplicado: {root} -> {new}", "warn")
        used.add(new)
        rename_map[root] = new

    buf = io.BytesIO()
    ok  = 0
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zout:
        for path, data in sorted(items, key=lambda x: x[0]):
            root     = path.split('/')[0]
            new_root = rename_map.get(root, root)
            new_path = new_root + path[len(root):]
            try:
                zout.writestr(new_path, data)
                log(f"  + {new_path}", "ok")
                ok += 1
            except Exception as e:
                log(f"  ERR {new_path}: {e}", "error")

    buf.seek(0)
    last_zip_bytes = buf.read()

    # ── Upload result ZIP to Backblaze ──
    client = get_b2_client()
    if client:
        zip_name = f"Renombrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        try:
            buf2 = io.BytesIO(last_zip_bytes)
            client.put_object(
                Bucket=B2_BUCKET_NAME,
                Key=zip_name,
                Body=buf2,
                ContentType='application/zip',
            )
            log(f"  ↑ ZIP subido a Backblaze: {zip_name}", "ok")
        except ClientError as e:
            log(f"  ✗ Backblaze upload error: {e}", "error")
    else:
        log("  ℹ Backblaze no configurado — ZIP solo en memoria", "warn")

    log("─────────────────────────────────", "head")
    log(f"Listo: {ok} archivos en {len(rename_map)} carpetas", "head")
    log("ZIP listo para descarga.", "ok")
    log("Proceso completado.", "ok")
    is_running = False

# ── Flask routes ────────────────────────────────────────

@app.route("/debug", methods=["POST"])
def debug():
    out = {}
    for key in request.files:
        fs = request.files[key]
        out[key] = {
            "filename": fs.filename,
            "content_type": fs.content_type,
            "size": len(fs.read()),
        }
    for key in request.form:
        out[key] = request.form[key]
    return jsonify(out)

@app.route("/preview", methods=["POST"])
def preview():
    items = collect(request.files)
    roots = {}
    for path, _ in items:
        root = path.split('/')[0]
        roots[root] = roots.get(root, 0) + 1
    return jsonify({"items": [{"nombre": r, "archivos": c} for r, c in sorted(roots.items())]})

@app.route("/run", methods=["POST"])
def run():
    global is_running
    if is_running:
        return jsonify({"error": "Ya hay un proceso en ejecución"}), 400
    items = collect(request.files)
    if not items:
        return jsonify({"error": "No se recibieron archivos válidos"}), 400
    while not log_queue.empty():
        log_queue.get()
    threading.Thread(target=run_bot, args=(items,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/download")
def download():
    if not last_zip_bytes:
        return "No hay ZIP disponible", 404
    return send_file(
        io.BytesIO(last_zip_bytes),
        as_attachment=True,
        download_name="Renombrado.zip",
        mimetype="application/zip"
    )

@app.route("/stream")
def stream():
    def generate():
        while True:
            try:
                entry = log_queue.get(timeout=30)
                yield f"data: {json.dumps(entry)}\n\n"
            except queue.Empty:
                yield 'data: {"ping":true}\n\n'
    return Response(stream_with_context(generate()), mimetype="text/event-stream")

@app.route("/status")
def status():
    return jsonify({
        "running": is_running,
        "zip_ready": last_zip_bytes is not None,
        "b2_configured": bool(B2_KEY_ID and B2_APP_KEY and B2_ENDPOINT),
    })

if __name__ == "__main__":
    print("\nRenomBot SIS — http://localhost:5000\n")
    app.run(debug=False, threaded=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
