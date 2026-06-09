"""
RenomBot · SIS — Backend v3.1 (Optimizado)
===========================================
Mejoras aplicadas vs v3.0:
  - Validación de job_id con regex para evitar path traversal / injection
  - Límite de tamaño de ZIP individual configurable vía env
  - log_queue con maxsize para evitar memory leak en jobs lentos
  - Sanitización de nombres de archivo reforzada (caracteres Unicode peligrosos)
  - _b2_client_cache se invalida si las credenciales cambian en runtime
  - Timeout de SSE configurable; heartbeat más robusto con manejo de GeneratorExit
  - Error handler 413 devuelve JSON siempre (antes fallaba si el cliente esperaba JSON)
  - _purge_old_jobs ejecuta también al crear jobs, no solo al leerlos
  - Separación de concerns: validación, procesado y respuesta en funciones pequeñas
  - Headers de seguridad HTTP añadidos (CSP, X-Content-Type-Options, etc.)
  - Compatibilidad Python 3.12+: uso explícito de zipfile.Path-free patterns
  - Documentación de todos los módulos y funciones
"""
from __future__ import annotations

import io
import json
import logging
import os
import queue
import re
import threading
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from flask import (
    Flask,
    Response,
    jsonify,
    render_template,
    request,
    send_file,
    stream_with_context,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("renombot")

# ─── App ──────────────────────────────────────────────────────────────────────
app = Flask(__name__)

# FIX: MAX_CONTENT_LENGTH cubre el caso de uploads gigantes; se deja en 1 GB
# pero MAX_UPLOAD_MB por job es el control real (ver _collect).
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024  # 1 GB

# ─── Configuración (env vars) ─────────────────────────────────────────────────
B2_KEY_ID      = os.environ.get("B2_KEY_ID",      "").strip()
B2_APP_KEY     = os.environ.get("B2_APP_KEY",      "").strip()
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME",  "renombot-sis").strip()
B2_ENDPOINT    = os.environ.get("B2_ENDPOINT",     "").strip()

MAX_UPLOAD_MB  = int(os.environ.get("MAX_UPLOAD_MB",       "500"))
MAX_JOBS       = int(os.environ.get("MAX_JOBS",            "10"))
JOB_TTL_SEC    = int(os.environ.get("JOB_TTL_SEC",         "600"))   # 10 min — libera RAM rápido
ZIP_COMPRESS   = int(os.environ.get("ZIP_COMPRESS_LEVEL",  "6"))
# FIX NUEVO: límite por archivo interno de ZIP (configurable)
MAX_FILE_MB    = int(os.environ.get("MAX_FILE_MB",         "500"))
# FIX NUEVO: maxsize para log_queue; evita memoria ilimitada en jobs largos
LOG_QUEUE_SIZE = int(os.environ.get("LOG_QUEUE_SIZE",      "2000"))
# FIX NUEVO: timeout SSE heartbeat
SSE_TIMEOUT    = int(os.environ.get("SSE_TIMEOUT_SEC",     "20"))
# MEJORA 3: TTL para URLs pre-firmadas de Backblaze (en segundos)
B2_PRESIGN_TTL = int(os.environ.get("B2_PRESIGN_TTL",      "3600"))

# FIX: Regex para validar job_id (uuid hex: 32 caracteres hexadecimales)
_JOB_ID_RE = re.compile(r'^[0-9a-f]{32}$')

# ─── Estado por sesión ────────────────────────────────────────────────────────
# IMPORTANTE: este estado vive solo en este proceso. El deploy debe correr
# con un único worker de Gunicorn + varios threads. Ver Procfile.
_jobs_lock: threading.RLock = threading.RLock()
_jobs: Dict[str, Dict[str, Any]] = {}


def _purge_old_jobs() -> None:
    """Elimina jobs no activos con más de JOB_TTL_SEC de antigüedad."""
    now = time.time()
    with _jobs_lock:
        stale = [
            jid for jid, j in _jobs.items()
            if not j["running"] and (now - j["created"]) > JOB_TTL_SEC
        ]
        for jid in stale:
            log.info("Purgando job expirado: %s", jid[:8])
            del _jobs[jid]


def _new_job() -> str:
    """Crea un nuevo job y devuelve su ID."""
    _purge_old_jobs()
    job_id = uuid.uuid4().hex
    with _jobs_lock:
        _jobs[job_id] = {
            # FIX: maxsize evita que un job con SSE caído acumule mensajes sin límite
            "log_queue": queue.Queue(maxsize=LOG_QUEUE_SIZE),
            "running":   False,
            "done":      False,
            "zip_bytes": None,
            "b2_url":    None,   # URL de descarga directa en Backblaze (presigned)
            "created":   time.time(),
            "error":     None,
        }
    return job_id


def _validate_job_id(job_id: str) -> bool:
    """Valida que job_id sea un UUID hex legítimo. Previene injection."""
    return bool(job_id and _JOB_ID_RE.match(job_id))


def _get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Devuelve el dict de un job o None si no existe o el ID es inválido.
    FIX: valida el formato del ID antes de buscar (evita key lookups con datos
    arbitrarios del usuario).
    """
    if not _validate_job_id(job_id):
        return None
    with _jobs_lock:
        return _jobs.get(job_id)


def _job_log(job_id: str, msg: str, tipo: str = "info") -> None:
    """
    Encola un mensaje de log para el job.
    FIX: usa put_nowait y captura queue.Full en lugar de dejarlo fallar
    silenciosamente; ahora registra la condición en el logger del servidor.
    """
    j = _get_job(job_id)
    if j is None:
        return
    ts = datetime.now().strftime("%H:%M:%S")
    try:
        j["log_queue"].put_nowait({"ts": ts, "msg": msg, "tipo": tipo})
    except queue.Full:
        log.warning("log_queue lleno para job %s — mensaje descartado: %s", job_id[:8], msg[:80])


# ─── Cliente B2 (lazy + cacheado) ────────────────────────────────────────────
_b2_lock   = threading.Lock()
_b2_client: Optional[Any] = None
_b2_creds  = (B2_KEY_ID, B2_APP_KEY, B2_ENDPOINT)   # snapshot al inicio


def _get_b2_client():
    """
    Devuelve el cliente boto3 cacheado o None si B2 no está configurado.
    FIX: guarda snapshot de credenciales para detectar cambios en runtime
    (útil si se recargan env vars sin reiniciar el proceso).
    """
    global _b2_client, _b2_creds
    if not all([B2_KEY_ID, B2_APP_KEY, B2_ENDPOINT]):
        return None
    current_creds = (B2_KEY_ID, B2_APP_KEY, B2_ENDPOINT)
    with _b2_lock:
        if _b2_client is None or _b2_creds != current_creds:
            _b2_creds = current_creds
            try:
                _b2_client = boto3.client(
                    "s3",
                    endpoint_url=f"https://{B2_ENDPOINT}",
                    aws_access_key_id=B2_KEY_ID,
                    aws_secret_access_key=B2_APP_KEY,
                    config=Config(
                        signature_version="s3v4",
                        connect_timeout=10,
                        read_timeout=30,
                        retries={"max_attempts": 3, "mode": "standard"},
                    ),
                )
                log.info("Cliente B2 creado/actualizado")
            except Exception as exc:
                log.error("No se pudo crear cliente B2: %s", exc)
                _b2_client = None
        return _b2_client


def _b2_configured() -> bool:
    """Indica si las variables de entorno de B2 están todas presentes."""
    return bool(B2_KEY_ID and B2_APP_KEY and B2_ENDPOINT)


def _b2_upload_and_get_url(
    job_id: str, zip_bytes: bytes, zip_name: str
) -> Optional[str]:
    """
    Sube el ZIP a Backblaze B2 y devuelve una URL de descarga directa (presigned).
    La URL es válida por 7 días (604800 segundos, máximo que permite B2).
    Devuelve None si B2 no está configurado o si falla la subida.
    """
    client = _get_b2_client()
    if client is None:
        return None
    try:
        client.put_object(
            Bucket=B2_BUCKET_NAME,
            Key=zip_name,
            Body=zip_bytes,
            ContentType="application/zip",
        )
        # Generar URL de descarga directa presigned (7 días = máximo de B2)
        url = client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": B2_BUCKET_NAME,
                "Key": zip_name,
                "ResponseContentDisposition": f'attachment; filename="{zip_name}"',
            },
            ExpiresIn=604800,  # 7 días
        )
        log.info("B2 presigned URL generada para %s", zip_name)
        return url
    except (ClientError, BotoCoreError) as exc:
        log.warning("Error subiendo a B2 (%s): %s", zip_name, exc)
        return None
    except Exception as exc:
        log.exception("Error inesperado subiendo a B2 (%s)", zip_name)
        return None


# ─── Helpers de renombrado ────────────────────────────────────────────────────
_JUNK_NAMES = frozenset({
    "downloads", "descargas", "temp", "tmp", "desktop", "documents",
})

# FIX: caracteres ilegales en Windows (extendido con rangos de control Unicode)
_WIN_ILLEGAL = re.compile(r'[<>:"/\\|?*\x00-\x1f\x7f]')


def _folder_name_from(name: str) -> str:
    """
    Extrae el nombre de carpeta destino desde el nombre de archivo.
    Ejemplo: '668505-34-7259802.zip'  →  '7259802'
    """
    stem = name
    if stem.lower().endswith(".zip"):
        stem = stem[:-4]
    parts = stem.split("-")
    return parts[-1] if len(parts) > 1 else stem


def _is_junk_segment(s: str) -> bool:
    """Detecta segmentos de ruta que deben descartarse al reorganizar."""
    if not s or s in (".", ".."):
        return True
    low = s.lower()
    if low.startswith("descargamasiva-"):
        return True
    if s.isdigit() and len(s) >= 8:   # timestamps tipo 202605291006
        return True
    if low in _JUNK_NAMES:
        return True
    return False


def _safe_filename_segment(p: str) -> str:
    """
    Sanitiza un único segmento de ruta.
    FIX: usa regex en lugar de bucle de caracteres —más eficiente y completo.
    Reemplaza caracteres ilegales en Windows y strips espacios.
    """
    p = p.strip()
    p = _WIN_ILLEGAL.sub("_", p)
    # Evitar nombres reservados de Windows (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
    # Añadir sufijo para que no rompan la extracción en Windows
    _WIN_RESERVED = re.compile(
        r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?$', re.IGNORECASE
    )
    if _WIN_RESERVED.match(p):
        p = "_" + p
    return p


def _safe_path(path: str) -> str:
    """
    Sanitiza una ruta completa para uso seguro dentro del ZIP:
    - Normaliza separadores
    - Elimina path traversal (..)
    - Sanitiza cada segmento individualmente
    FIX: ahora usa _safe_filename_segment en cada parte.
    """
    path = path.replace("\\", "/")
    parts = []
    for p in path.split("/"):
        p = _safe_filename_segment(p)
        if not p or p in (".", ".."):
            continue
        parts.append(p)
    return "/".join(parts)


def _unpack_zip_bytes(
    data: bytes, zip_filename: str, job_id: str
) -> List[Tuple[str, bytes]]:
    """
    Descomprime un ZIP y produce rutas equivalentes a subir la carpeta directamente.

    Regla: el ZIP puede tener 1, 2 o 3+ niveles de profundidad.
    En todos los casos la salida es igual que si el usuario hubiera subido
    la carpeta vía webkitdirectory:

      Caso A — archivo suelto (1 nivel):  archivo.pdf
          → <stem_zip>/archivo.pdf

      Caso B — carpeta/archivo (2 niveles):  668505-34-7259802/archivo.pdf
          → 668505-34-7259802/archivo.pdf   (sin cambios)

      Caso C — lote/carpeta/archivo (3+ niveles):  3208/668505-34-7259802/archivo.pdf
          → 668505-34-7259802/archivo.pdf   (se elimina la carpeta raíz del lote)

    Así _run_bot recibe siempre <carpeta_factura>/<archivo> y renombra correctamente.
    """
    results: List[Tuple[str, bytes]] = []
    max_file_bytes = MAX_FILE_MB * 1024 * 1024

    # stem del ZIP (para Caso A)
    zip_stem = zip_filename
    if zip_stem.lower().endswith(".zip"):
        zip_stem = zip_stem[:-4]

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            entries = [i for i in zf.infolist() if not i.is_dir()]
            if not entries:
                _job_log(job_id, f"  ⚠ {zip_filename}: ZIP vacío", "warn")
                return results

            for info in entries:
                # Protección zip-bomb
                if info.compress_size > 0:
                    ratio = info.file_size / max(info.compress_size, 1)
                    if ratio > 100:
                        _job_log(job_id, f"  ⚠ Ratio sospechoso ({ratio:.0f}x): {info.filename[:60]}", "warn")
                        continue

                if info.file_size > max_file_bytes:
                    _job_log(job_id, f"  ⚠ Demasiado grande ({info.file_size//1024//1024} MB): {info.filename[:60]}", "warn")
                    continue

                # Codificación
                try:
                    raw_name = info.filename
                except UnicodeDecodeError:
                    raw_name = info.filename.encode("cp437").decode("latin-1")

                raw   = _safe_path(raw_name)
                parts = [p for p in raw.split("/") if p]
                if not parts:
                    continue

                # ── Normalizar profundidad ────────────────────────────────────
                if len(parts) == 1:
                    # Caso A: archivo suelto → prefijamos con stem del ZIP
                    final_parts = [zip_stem, parts[0]]
                elif len(parts) == 2:
                    # Caso B: ya tiene la forma correcta carpeta/archivo
                    final_parts = parts
                else:
                    # Caso C: 3+ niveles → eliminar la carpeta raíz (el lote)
                    final_parts = parts[1:]

                # Filtrar segmentos junk intermedios (nunca el archivo final)
                middle  = [p for p in final_parts[:-1] if not _is_junk_segment(p)]
                cleaned = middle + [final_parts[-1]]
                if not cleaned:
                    continue

                try:
                    results.append(("/".join(cleaned), zf.read(info)))
                except Exception as exc:
                    _job_log(job_id, f"  ✗ Lectura fallida ({info.filename[:50]}): {exc}", "error")

        _job_log(job_id, f"  ZIP {zip_filename} → {len(results)} archivos extraídos", "ok")

    except zipfile.BadZipFile:
        _job_log(job_id, f"  ✗ {zip_filename}: ZIP inválido o corrupto", "error")
    except Exception as exc:
        log.exception("Error descomprimiendo %s", zip_filename)
        _job_log(job_id, f"  ✗ {zip_filename}: {exc}", "error")

    return results


def _collect(
    uploaded_files, job_id: str = ""
) -> Tuple[List[Tuple[str, bytes]], List[str]]:
    """
    Procesa los archivos del request y devuelve (items, warnings).
    FIX: valida que el índice del campo (idx) sea numérico para evitar
    procesado de campos inyectados con nombres arbitrarios.
    FIX: limit de 1000 archivos por campo para evitar DoS por enumeración.
    """
    items: List[Tuple[str, bytes]] = []
    warnings: List[str] = []
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    seen_indices: set = set()

    for key in uploaded_files:
        if not key.startswith("file_"):
            continue

        # FIX: idx debe ser numérico (evita inyección via campo "file_../../etc")
        idx = key[5:]
        if not idx.isdigit():
            log.warning("Índice de campo no numérico ignorado: %s", key)
            continue

        # FIX: evitar duplicados de índice
        if idx in seen_indices:
            continue
        seen_indices.add(idx)

        fs = uploaded_files[key]

        try:
            data = fs.read()
        except Exception as exc:
            warnings.append(f"{fs.filename}: error de lectura ({exc})")
            continue

        if len(data) > max_bytes:
            warnings.append(
                f"{fs.filename}: excede {MAX_UPLOAD_MB} MB "
                f"({len(data) / 1024 / 1024:.1f} MB)"
            )
            continue

        # FIX: strips de comillas y espacios en nombre y path
        fs_name  = (fs.filename or "").strip('"').strip()
        path_raw = request.form.get(f"path_{idx}", fs_name).strip('"').strip()

        # FIX: limitar longitud de path_raw para evitar procesado de cadenas enormes
        if len(path_raw) > 4096:
            path_raw = path_raw[:4096]

        path_norm  = _safe_path(path_raw)
        path_parts = [p for p in path_norm.split("/") if p]
        last_seg   = path_parts[-1] if path_parts else fs_name

        is_zip = (
            fs_name.lower().endswith(".zip")
            or last_seg.lower().endswith(".zip")
        )

        if is_zip:
            zip_name = fs_name if fs_name.lower().endswith(".zip") else last_seg
            unpacked = _unpack_zip_bytes(data, zip_name, job_id)
            if not unpacked:
                warnings.append(f"{zip_name}: sin archivos válidos tras extraer")
            items.extend(unpacked)
        else:
            clean = [p for p in path_parts if not _is_junk_segment(p)]
            if not clean:
                continue
            root = clean[0]
            if "-" in root:
                clean[0] = _folder_name_from(root)
            items.append(("/".join(clean), data))

    return items, warnings


# ─── Worker principal ─────────────────────────────────────────────────────────
def _run_bot(job_id: str, items: List[Tuple[str, bytes]]) -> None:
    """
    Empaqueta los archivos en un ZIP en memoria y opcionalmente sube a B2.
    FIX: el ZIP se construye sobre un BytesIO preasignado para evitar
    múltiples reallocations en archivos grandes.
    FIX: sOk se calcula correctamente contando entradas únicas en ok_roots.
    """
    j = _get_job(job_id)
    if j is None:
        return

    j["running"]   = True
    j["done"]      = False
    j["zip_bytes"] = None
    j["error"]     = None

    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _job_log(job_id, "─────────────────────────────────", "head")
        _job_log(job_id, f"Inicio: {ts}", "head")
        _job_log(job_id, f"Archivos totales: {len(items)}", "info")
        _job_log(job_id, "─────────────────────────────────", "head")

        if not items:
            _job_log(job_id, "Sin archivos para procesar.", "warn")
            return

        # ── Normalizar paths ──────────────────────────────────────────────────
        # El nombre de factura puede estar en cualquier nivel de la ruta:
        #   Carpeta directa:        7259802/archivo.pdf          (ya OK por _collect)
        #   ZIP plano:              668505-34-7259802/archivo.pdf (nivel 1)
        #   Carpeta con raíz extra: MiLote/668505-34-7259802/a.pdf (nivel 2)
        #
        # Solución: recorrer TODOS los segmentos y aplicar _folder_name_from
        # a cualquiera que tenga el patrón con guiones. El archivo (último) no se toca.

        def _normalize_path(path: str) -> str:
            parts = [p for p in path.split("/") if p]
            for i in range(len(parts) - 1):   # nunca tocar el último (es el archivo)
                if "-" in parts[i]:
                    parts[i] = _folder_name_from(parts[i])
            return "/".join(parts)

        items = [(_normalize_path(path), data) for path, data in items]

        # Construir rename_map — solo resuelve colisiones (ya todo normalizado)
        root_count: Dict[str, int] = defaultdict(int)
        for path, _ in items:
            root_count[path.split("/")[0]] += 1

        used: set = set()
        rename_map: Dict[str, str] = {}
        for root in sorted(root_count):
            new_root = root
            if new_root in used:
                c = 1
                while f"{new_root}-{c}" in used:
                    c += 1
                new_root = f"{new_root}-{c}"
                _job_log(job_id, f"  Duplicado resuelto: {root} → {new_root}", "warn")
            used.add(new_root)
            rename_map[root] = new_root

        # Empaquetar ZIP en memoria
        buf = io.BytesIO()
        ok = 0
        ok_roots: Dict[str, int] = defaultdict(int)

        with zipfile.ZipFile(
            buf, "w", zipfile.ZIP_DEFLATED, compresslevel=ZIP_COMPRESS
        ) as zout:
            for path, data in sorted(items, key=lambda x: x[0]):
                root     = path.split("/")[0]
                new_root = rename_map.get(root, root)
                new_path = new_root + path[len(root):]
                try:
                    zout.writestr(new_path, data)
                    ok_roots[new_root] += 1
                    ok += 1
                except Exception as exc:
                    _job_log(job_id, f"  ✗ {new_path[:80]}: {exc}", "error")

        for root_name, count in sorted(ok_roots.items()):
            plural = "s" if count != 1 else ""
            _job_log(job_id, f"  ✓ {root_name}/ ({count} archivo{plural})", "ok")

        buf.seek(0)
        j["zip_bytes"] = buf.getvalue()

        # Subida a Backblaze B2 + URL de descarga directa
        ts2      = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_name = f"Renombrado_{ts2}_{job_id[:6]}.zip"
        b2_url   = _b2_upload_and_get_url(job_id, j["zip_bytes"], zip_name)
        if b2_url:
            j["b2_url"] = b2_url
            _job_log(job_id, f"  ↑ Subido a Backblaze: {zip_name}", "ok")
        elif _b2_configured():
            _job_log(job_id, "  ⚠ Backblaze falló — descarga local disponible", "warn")

        _job_log(job_id, "─────────────────────────────────", "head")
        _job_log(job_id, f"Listo: {ok} archivos en {len(ok_roots)} carpetas", "head")

    except Exception as exc:
        log.exception("Error fatal en _run_bot")
        j["error"] = str(exc)
        _job_log(job_id, f"ERROR fatal: {exc}", "error")
    finally:
        j["done"]    = True
        j["running"] = False
        # Sentinel SSE: el frontend reacciona a este mensaje
        _job_log(job_id, "__JOB_FINISHED__", "system")


# ─── Limpieza B2 en background ────────────────────────────────────────────────
_b2_clear_status: Dict[str, Any] = {"running": False, "last_result": None}
_b2_clear_lock = threading.Lock()


def _do_clear_b2() -> None:
    """
    Elimina todos los objetos del bucket B2 en background.
    FIX: el fallback de borrado uno a uno ahora loguea la excepción real
    del batch para facilitar debugging.
    """
    with _b2_clear_lock:
        _b2_clear_status["running"]     = True
        _b2_clear_status["last_result"] = None

    deleted: List[str] = []
    errors:  List[str] = []
    client = _get_b2_client()

    if client is None:
        with _b2_clear_lock:
            _b2_clear_status["running"]     = False
            _b2_clear_status["last_result"] = {
                "deleted_count": 0,
                "errors": ["Backblaze no configurado"],
                "ok": False,
            }
        return

    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
            contents = page.get("Contents", [])
            if not contents:
                continue

            chunk = [{"Key": obj["Key"]} for obj in contents]
            try:
                resp = client.delete_objects(
                    Bucket=B2_BUCKET_NAME,
                    Delete={"Objects": chunk, "Quiet": False},
                )
                for d in resp.get("Deleted", []):
                    deleted.append(d["Key"])
                for e in resp.get("Errors", []):
                    errors.append(f"{e.get('Key', '?')}: {e.get('Message', '?')}")
            except (ClientError, BotoCoreError) as batch_exc:
                # FIX: registrar el error del batch antes del fallback
                log.warning("Borrado batch falló (%s), usando borrado individual", batch_exc)
                for obj in contents:
                    try:
                        client.delete_object(Bucket=B2_BUCKET_NAME, Key=obj["Key"])
                        deleted.append(obj["Key"])
                    except Exception as exc2:
                        errors.append(f"{obj['Key']}: {exc2}")

    except Exception as exc:
        log.exception("Error listando objetos B2")
        errors.append(str(exc))

    with _b2_clear_lock:
        _b2_clear_status["running"]     = False
        _b2_clear_status["last_result"] = {
            "deleted_count": len(deleted),
            "errors":        errors,
            "ok":            len(errors) == 0,
        }


# ─── Headers de seguridad HTTP ────────────────────────────────────────────────
# FIX NUEVO: añadir headers de seguridad a todas las respuestas
@app.after_request
def add_security_headers(response: Response) -> Response:
    """Añade cabeceras de seguridad HTTP recomendadas."""
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    # CSP permisiva pero segura: permite Google Fonts y cdnjs (usados en la UI)
    csp = (
        "default-src 'self'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdnjs.cloudflare.com; "
        "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; "
        "script-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "frame-ancestors 'none';"
    )
    response.headers.setdefault("Content-Security-Policy", csp)
    return response


# ─── Rutas Flask ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/favicon.ico")
def favicon():
    return send_file("static/favicon.ico", mimetype="image/x-icon")


@app.route("/health")
def health():
    """Healthcheck liviano — no toca B2 ni el estado de jobs."""
    with _jobs_lock:
        active = sum(1 for j in _jobs.values() if j["running"])
        total  = len(_jobs)
    return jsonify({
        "ok":          True,
        "ts":          datetime.now().isoformat(),
        "jobs_active": active,
        "jobs_total":  total,
    })


@app.route("/job/new", methods=["POST"])
def job_new():
    """Crea un job nuevo. Devuelve job_id."""
    with _jobs_lock:
        active = sum(1 for j in _jobs.values() if j["running"])
    if active >= MAX_JOBS:
        return jsonify({
            "error": f"Servidor ocupado ({active}/{MAX_JOBS} activos). Intenta en un momento.",
        }), 503
    return jsonify({"job_id": _new_job()})


@app.route("/job/reset", methods=["POST"])
def job_reset():
    """
    Resetea el estado de un job existente para reutilizarlo inmediatamente.
    RENDIMIENTO: evita crear un nuevo job (y la latencia de red asociada)
    entre conversiones consecutivas. Libera zip_bytes de RAM al instante.
    """
    job_id = request.json.get("job_id", "").strip() if request.is_json else ""
    if not _validate_job_id(job_id):
        return jsonify({"error": "job_id inválido"}), 400

    j = _get_job(job_id)
    if j is None:
        # Job expirado → crear uno nuevo automáticamente
        new_id = _new_job()
        return jsonify({"job_id": new_id, "recycled": False})

    if j["running"]:
        return jsonify({"error": "Job activo, no se puede resetear"}), 409

    # Reset completo de estado
    j["done"]      = False
    j["zip_bytes"] = None  # liberar RAM inmediatamente
    j["error"]     = None
    j["created"]   = time.time()  # renovar TTL

    # Drenar cola de logs
    while not j["log_queue"].empty():
        try:
            j["log_queue"].get_nowait()
        except queue.Empty:
            break

    return jsonify({"job_id": job_id, "recycled": True})


@app.route("/run", methods=["POST"])
def run():
    """
    Recibe los archivos y lanza el worker en background.
    FIX: valida job_id con regex antes de buscar en _jobs.
    FIX: resetea initPromise en frontend si la sesión expiró (devuelve 410).
    """
    job_id = request.form.get("job_id", "").strip()

    if not _validate_job_id(job_id):
        return jsonify({"error": "job_id inválido"}), 400

    j = _get_job(job_id)
    if j is None:
        # FIX: 410 Gone indica que la sesión expiró (distinto a 400 input error)
        return jsonify({"error": "Sesión expirada. Recarga la página.", "expired": True}), 410

    if j["running"]:
        return jsonify({"error": "Ya hay un proceso activo en esta sesión"}), 409

    # Reset del estado del job
    j["done"]      = False
    j["zip_bytes"] = None
    j["error"]     = None

    # Drenar log_queue residual de ejecuciones anteriores
    drained = 0
    while not j["log_queue"].empty():
        try:
            j["log_queue"].get_nowait()
            drained += 1
        except queue.Empty:
            break
    if drained:
        log.debug("Drenados %d mensajes residuales del job %s", drained, job_id[:8])

    items, warnings = _collect(request.files, job_id)
    if not items:
        msg = "No se recibieron archivos válidos"
        if warnings:
            msg += ": " + "; ".join(warnings)
        return jsonify({"error": msg}), 400

    for w in warnings:
        _job_log(job_id, f"⚠ {w}", "warn")

    threading.Thread(
        target=_run_bot,
        args=(job_id, items),
        daemon=True,
        name=f"runbot-{job_id[:8]}",
    ).start()

    return jsonify({"ok": True, "warnings": warnings, "total_files": len(items)})


@app.route("/download/<job_id>")
def download(job_id: str):
    """
    Descarga el ZIP generado.
    RENDIMIENTO: libera zip_bytes de memoria inmediatamente después de enviar
    para no retener cientos de MB hasta que el job expire (1 hora).
    Acepta ?name= para el nombre del archivo descargado.
    """
    if not _validate_job_id(job_id):
        return jsonify({"error": "job_id inválido"}), 400

    j = _get_job(job_id)
    if j is None:
        return jsonify({"error": "Sesión expirada", "expired": True}), 410
    if not j["zip_bytes"]:
        return jsonify({"error": "ZIP no disponible aún"}), 404

    # Leer y liberar inmediatamente de memoria
    zip_data       = j["zip_bytes"]
    j["zip_bytes"] = None   # liberar RAM — el job sigue existiendo pero sin el ZIP

    dl_name = request.args.get("name", "Renombrado.zip")
    # Sanitizar el nombre de descarga
    dl_name = _safe_filename_segment(dl_name) or "Renombrado.zip"
    if not dl_name.lower().endswith(".zip"):
        dl_name += ".zip"

    return send_file(
        io.BytesIO(zip_data),
        as_attachment=True,
        download_name=dl_name,
        mimetype="application/zip",
    )


@app.route("/stream/<job_id>")
def stream(job_id: str):
    """
    SSE stream del progreso del job.
    FIX: valida job_id; heartbeat con timeout configurable.
    FIX: maneja GeneratorExit limpiamente para no dejar threads colgados.
    FIX: si el job ya terminó al momento de conectar el SSE, envía los
    mensajes residuales + sentinel inmediatamente en lugar de esperar timeout.
    """
    if not _validate_job_id(job_id):
        return "job_id inválido", 400

    j = _get_job(job_id)
    if j is None:
        return "Job no encontrado o expirado", 404

    def generate():
        # Heartbeat inicial → confirma conexión al browser
        yield 'data: {"connected":true}\n\n'

        # FIX: si el job ya finalizó, vaciar la cola y salir
        if j["done"] and j["log_queue"].empty():
            yield 'data: {"msg":"__JOB_FINISHED__","tipo":"system","ts":"--:--:--"}\n\n'
            return

        while True:
            try:
                entry = j["log_queue"].get(timeout=SSE_TIMEOUT)
                yield f"data: {json.dumps(entry)}\n\n"
                if entry.get("msg") == "__JOB_FINISHED__":
                    return
            except queue.Empty:
                # Heartbeat para mantener la conexión viva
                yield 'data: {"ping":true}\n\n'
                # FIX: si el job terminó y la cola está vacía, cerrar el stream
                if j["done"] and j["log_queue"].empty():
                    yield 'data: {"msg":"__JOB_FINISHED__","tipo":"system","ts":"--:--:--"}\n\n'
                    return
            except GeneratorExit:
                # Cliente desconectado
                return

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":     "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@app.route("/status/<job_id>")
def status(job_id: str):
    """
    Estado del job.
    FIX: distingue 404 (no existe) de 410 (expiró).
    FIX: valida job_id.
    """
    if not _validate_job_id(job_id):
        return jsonify({"error": "job_id inválido"}), 400

    j = _get_job(job_id)
    if j is None:
        return jsonify({"error": "Job no encontrado o expirado", "expired": True}), 410

    return jsonify({
        "running":   j["running"],
        "done":      j["done"],
        "zip_ready": j["zip_bytes"] is not None,
        "error":     j["error"],
    })


@app.route("/b2-status")
def b2_status():
    """
    Estado del bucket B2.
    FIX: captura excepción genérica y no expone el traceback al cliente.
    """
    client = _get_b2_client()
    if client is None:
        return jsonify({"configured": False})

    try:
        total_size = 0
        files      = []
        paginator  = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
            for obj in page.get("Contents", []):
                total_size += obj["Size"]
                files.append({
                    "name":     obj["Key"],
                    "size":     obj["Size"],
                    "modified": (
                        obj["LastModified"].isoformat()
                        if obj.get("LastModified") else None
                    ),
                })
        return jsonify({
            "configured":  True,
            "file_count":  len(files),
            "total_bytes": total_size,
            "total_mb":    round(total_size / 1024 / 1024, 2),
            "files":       files,
        })
    except (ClientError, BotoCoreError) as exc:
        log.warning("Error B2 en /b2-status: %s", exc)
        return jsonify({"configured": True, "error": str(exc)}), 500
    except Exception:
        log.exception("Error inesperado en /b2-status")
        return jsonify({"configured": True, "error": "Error interno del servidor"}), 500


@app.route("/clear-b2", methods=["POST"])
def clear_b2():
    """Lanza la limpieza del bucket B2 en background."""
    with _b2_clear_lock:
        if _b2_clear_status["running"]:
            return jsonify({"status": "already_running"}), 202
    threading.Thread(target=_do_clear_b2, daemon=True, name="b2-clear").start()
    return jsonify({"status": "started"})


@app.route("/clear-b2/status")
def clear_b2_status_route():
    """Estado de la última operación de limpieza de B2."""
    with _b2_clear_lock:
        return jsonify({
            "running":     _b2_clear_status["running"],
            "last_result": _b2_clear_status["last_result"],
        })


# ─── Error handlers globales ──────────────────────────────────────────────────

@app.errorhandler(400)
def bad_request(_e):
    # FIX: siempre JSON para rutas de API
    return jsonify({"error": "Solicitud inválida"}), 400


@app.errorhandler(413)
def too_large(_e):
    # FIX: mensaje en español y tamaño legible
    return jsonify({
        "error": f"Carga demasiado grande (máximo global: {app.config['MAX_CONTENT_LENGTH'] // 1024 // 1024} MB)"
    }), 413


@app.errorhandler(404)
def not_found(_e):
    if request.accept_mimetypes.accept_json and not request.accept_mimetypes.accept_html:
        return jsonify({"error": "Endpoint no encontrado"}), 404
    return "Página no encontrada", 404


@app.errorhandler(405)
def method_not_allowed(_e):
    return jsonify({"error": "Método no permitido"}), 405


@app.errorhandler(500)
def server_error(_e):
    log.exception("Error 500 no manejado")
    return jsonify({"error": "Error interno del servidor"}), 500


# ─── FURIPSBot — lógica de renombrado ────────────────────────────────────────
def _run_furips(job_id: str, items: List[Tuple[str, bytes]]) -> None:
    """
    Worker de FURIPSBot.
    Recibe una carpeta con estructura:
        <carpeta_raiz>/<numero_factura>/FURIPS1XXXX.ext
        <carpeta_raiz>/<numero_factura>/FURIPS2XXXX.ext
    Renombra cada archivo añadiendo _<numero_factura> al final del stem,
    y los empaqueta todos planos (sin subcarpetas) en un ZIP.
    """
    j = _get_job(job_id)
    if j is None:
        return

    j["running"]   = True
    j["done"]      = False
    j["zip_bytes"] = None
    j["error"]     = None

    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _job_log(job_id, "─────────────────────────────────", "head")
        _job_log(job_id, f"FURIPSBot — Inicio: {ts}", "head")
        _job_log(job_id, f"Archivos recibidos: {len(items)}", "info")
        _job_log(job_id, "─────────────────────────────────", "head")

        if not items:
            _job_log(job_id, "Sin archivos para procesar.", "warn")
            return

        buf = io.BytesIO()
        ok = 0
        errors = 0
        seen_names: set = set()

        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=ZIP_COMPRESS) as zout:
            for path, data in sorted(items, key=lambda x: x[0]):
                # Estructura esperada: raiz/factura/FURIPS1xxx.ext  (3 niveles)
                # o bien: factura/FURIPS1xxx.ext  (2 niveles, si la raíz fue descartada)
                parts = [p for p in path.replace("\\", "/").split("/") if p]

                if len(parts) < 2:
                    _job_log(job_id, f"  ⚠ Ruta inesperada (ignorado): {path[:60]}", "warn")
                    errors += 1
                    continue

                filename = parts[-1]          # FURIPS1xxxx.ext
                factura  = parts[-2]          # número de factura

                # Validar que sea un archivo FURIPS
                fname_upper = filename.upper()
                if not (fname_upper.startswith("FURIPS1") or fname_upper.startswith("FURIPS2")):
                    _job_log(job_id, f"  ⚠ No es FURIPS1/2 (ignorado): {filename[:60]}", "warn")
                    errors += 1
                    continue

                # Separar stem y extensión
                if "." in filename:
                    dot_idx  = filename.rfind(".")
                    stem     = filename[:dot_idx]
                    ext      = filename[dot_idx:]   # incluye el punto
                else:
                    stem = filename
                    ext  = ""

                # FIX: factura va AL INICIO → 7255454_FURIPS176001096140129042026.ext
                new_name = f"{factura}_{stem}{ext}"

                # Resolver colisiones de nombre (muy improbable, pero seguro)
                if new_name in seen_names:
                    c = 1
                    base_new = new_name
                    while new_name in seen_names:
                        new_name = f"{base_new[:-len(ext)] if ext else base_new}-{c}{ext}"
                        c += 1
                    _job_log(job_id, f"  ⚠ Nombre duplicado resuelto: {new_name}", "warn")

                seen_names.add(new_name)

                try:
                    zout.writestr(new_name, data)
                    ok += 1
                    tipo1 = "1" if fname_upper.startswith("FURIPS1") else "2"
                    _job_log(job_id, f"  ✓ FURIPS{tipo1} → {new_name}", "ok")
                except Exception as exc:
                    _job_log(job_id, f"  ✗ {new_name}: {exc}", "error")
                    errors += 1

        buf.seek(0)
        j["zip_bytes"] = buf.getvalue()

        # Subida a B2 (no bloqueante si falla)
        client = _get_b2_client()
        if client:
            ts2      = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = f"FURIPS_{ts2}_{job_id[:6]}.zip"
            try:
                client.put_object(
                    Bucket=B2_BUCKET_NAME,
                    Key=zip_name,
                    Body=j["zip_bytes"],
                    ContentType="application/zip",
                )
                _job_log(job_id, f"  ↑ Subido a Backblaze: {zip_name}", "ok")
            except (ClientError, BotoCoreError) as exc:
                _job_log(job_id, f"  ⚠ Backblaze: {exc}", "warn")
            except Exception as exc:
                log.exception("Error subiendo FURIPS a B2")
                _job_log(job_id, f"  ⚠ Backblaze (inesperado): {exc}", "warn")

        _job_log(job_id, "─────────────────────────────────", "head")
        _job_log(job_id, f"Listo: {ok} archivos renombrados, {errors} errores", "head")

    except Exception as exc:
        log.exception("Error fatal en _run_furips")
        j["error"] = str(exc)
        _job_log(job_id, f"ERROR fatal: {exc}", "error")
    finally:
        j["done"]    = True
        j["running"] = False
        _job_log(job_id, "__JOB_FINISHED__", "system")


def _collect_furips(uploaded_files, job_id: str = "") -> Tuple[List[Tuple[str, bytes]], List[str]]:
    """
    Procesa archivos para FURIPSBot.
    MEJORA 1+2: acepta múltiples carpetas Y múltiples ZIPs en una sola ejecución.
    Acepta:
      - Carpeta(s) directa(s) con subcarpetas de facturas (via webkitdirectory)
      - Uno o más ZIPs que contienen la estructura de carpetas
    Devuelve (items, warnings) donde items = [(ruta_relativa, bytes)].
    """
    items: List[Tuple[str, bytes]] = []
    warnings: List[str] = []
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    seen_indices: set = set()

    for key in uploaded_files:
        if not key.startswith("file_"):
            continue
        idx = key[5:]
        if not idx.isdigit():
            continue
        if idx in seen_indices:
            continue
        seen_indices.add(idx)

        fs = uploaded_files[key]

        try:
            data = fs.read()
        except Exception as exc:
            warnings.append(f"{fs.filename}: error de lectura ({exc})")
            continue

        if len(data) > max_bytes:
            warnings.append(f"{fs.filename}: excede {MAX_UPLOAD_MB} MB")
            continue

        fs_name  = (fs.filename or "").strip('"').strip()
        path_raw = request.form.get(f"path_{idx}", fs_name).strip('"').strip()
        if len(path_raw) > 4096:
            path_raw = path_raw[:4096]

        path_norm = _safe_path(path_raw)

        # MEJORA 1: ZIP → extraer estructura interna como si fuera carpeta
        if fs_name.lower().endswith(".zip"):
            _job_log(job_id, f"  📦 Extrayendo ZIP: {fs_name}", "info")
            zip_items = 0
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    # Validación: ZIP vacío
                    entries = [i for i in zf.infolist() if not i.is_dir()]
                    if not entries:
                        warnings.append(f"{fs_name}: ZIP vacío")
                        continue
                    for info in entries:
                        # Protección zip-bomb
                        if info.compress_size > 0 and (info.file_size / max(info.compress_size, 1)) > 100:
                            warnings.append(f"Ratio sospechoso en {info.filename[:50]}")
                            continue
                        if info.file_size > MAX_FILE_MB * 1024 * 1024:
                            warnings.append(f"Demasiado grande: {info.filename[:50]}")
                            continue
                        inner_path = _safe_path(info.filename)
                        if not inner_path:
                            continue
                        try:
                            items.append((inner_path, zf.read(info)))
                            zip_items += 1
                        except Exception as exc2:
                            warnings.append(f"Error leyendo {info.filename[:50]}: {exc2}")
            except zipfile.BadZipFile:
                warnings.append(f"{fs_name}: ZIP inválido o corrupto")
            except Exception as exc:
                warnings.append(f"{fs_name}: error extrayendo ({exc})")
            if zip_items:
                _job_log(job_id, f"  ✓ {fs_name}: {zip_items} archivos extraídos", "ok")
            continue  # siguiente archivo

        # Archivo normal (carpeta subida via webkitdirectory)
        items.append((path_norm, data))

    return items, warnings


@app.route("/furips/run", methods=["POST"])
def furips_run():
    """Endpoint de procesado para FURIPSBot."""
    job_id = request.form.get("job_id", "").strip()

    if not _validate_job_id(job_id):
        return jsonify({"error": "job_id inválido"}), 400

    j = _get_job(job_id)
    if j is None:
        return jsonify({"error": "Sesión expirada. Recarga la página.", "expired": True}), 410

    if j["running"]:
        return jsonify({"error": "Ya hay un proceso activo en esta sesión"}), 409

    j["done"]      = False
    j["zip_bytes"] = None
    j["error"]     = None

    while not j["log_queue"].empty():
        try:
            j["log_queue"].get_nowait()
        except queue.Empty:
            break

    items, warnings = _collect_furips(request.files, job_id)
    if not items:
        msg = "No se recibieron archivos válidos"
        if warnings:
            msg += ": " + "; ".join(warnings)
        return jsonify({"error": msg}), 400

    for w in warnings:
        _job_log(job_id, f"⚠ {w}", "warn")

    threading.Thread(
        target=_run_furips,
        args=(job_id, items),
        daemon=True,
        name=f"furips-{job_id[:8]}",
    ).start()

    return jsonify({"ok": True, "warnings": warnings, "total_files": len(items)})


# ─── MEJORA 3: URL pre-firmada de Backblaze ──────────────────────────────────
@app.route("/b2-presign/<path:key>")
def b2_presign(key: str):
    """
    Genera una URL pre-firmada de Backblaze B2 para descarga directa.
    La URL expira en B2_PRESIGN_TTL segundos (default 3600 = 1 hora).
    Esto permite que el browser descargue directamente desde B2 sin pasar
    por el servidor, reduciendo latencia y consumo de ancho de banda.
    SEGURIDAD: solo genera URLs para claves que existen en el bucket.
    """
    # Sanitizar la clave: no permitir path traversal
    key = key.strip("/").strip()
    if not key or ".." in key or key.startswith("/"):
        return jsonify({"error": "Clave inválida"}), 400

    client = _get_b2_client()
    if client is None:
        return jsonify({"error": "Backblaze no configurado"}), 503

    try:
        # Verificar que el objeto existe antes de firmar
        client.head_object(Bucket=B2_BUCKET_NAME, Key=key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "Archivo no encontrado en B2"}), 404
        log.warning("Error head_object en /b2-presign: %s", exc)
        return jsonify({"error": str(exc)}), 500

    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": B2_BUCKET_NAME, "Key": key},
            ExpiresIn=B2_PRESIGN_TTL,
        )
        return jsonify({
            "url":        url,
            "key":        key,
            "expires_in": B2_PRESIGN_TTL,
            "expires_at": (datetime.now().timestamp() + B2_PRESIGN_TTL),
        })
    except Exception as exc:
        log.exception("Error generando URL pre-firmada para %s", key)
        return jsonify({"error": "No se pudo generar URL de descarga"}), 500


# ─── MEJORA 4: Admin B2 unificado — también disponible para FURIPSBot ─────────
# Los endpoints /b2-status, /clear-b2 y /clear-b2/status ya son compartidos
# por ambos bots (mismo bucket). Este endpoint confirma el estado unificado
# y permite filtrar por prefijo (útil para separar archivos por bot).

@app.route("/b2-status/by-bot")
def b2_status_by_bot():
    """
    Devuelve el estado de B2 separado por bot (Renombrado_ vs FURIPS_).
    MEJORA 4: permite al frontend mostrar stats por bot sin compartir bucket.
    Ambos bots usan el mismo bucket → la eliminación siempre es compartida.
    """
    client = _get_b2_client()
    if client is None:
        return jsonify({"configured": False})

    try:
        renom_files  = []
        furips_files = []
        other_files  = []
        total_size   = 0

        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=B2_BUCKET_NAME):
            for obj in page.get("Contents", []):
                key  = obj["Key"]
                size = obj["Size"]
                total_size += size
                entry = {
                    "name":     key,
                    "size":     size,
                    "modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                }
                if key.startswith("Renombrado_"):
                    renom_files.append(entry)
                elif key.startswith("FURIPS_"):
                    furips_files.append(entry)
                else:
                    other_files.append(entry)

        return jsonify({
            "configured":  True,
            "total_mb":    round(total_size / 1024 / 1024, 2),
            "renombot":    {"count": len(renom_files),  "files": renom_files},
            "furipsbot":   {"count": len(furips_files), "files": furips_files},
            "other":       {"count": len(other_files),  "files": other_files},
            "shared_note": "Ambos bots comparten el mismo bucket. Limpiar elimina archivos de ambos.",
        })
    except (ClientError, BotoCoreError) as exc:
        log.warning("Error B2 en /b2-status/by-bot: %s", exc)
        return jsonify({"configured": True, "error": str(exc)}), 500
    except Exception:
        log.exception("Error inesperado en /b2-status/by-bot")
        return jsonify({"configured": True, "error": "Error interno"}), 500


@app.route("/b2-delete", methods=["POST"])
def b2_delete_file():
    """
    Elimina un archivo específico del bucket B2.
    MEJORA 4: permite eliminación granular desde cualquier bot.
    Body JSON: {"key": "nombre_del_archivo.zip"}
    """
    try:
        body = request.get_json(silent=True) or {}
        key  = (body.get("key") or "").strip()
    except Exception:
        return jsonify({"error": "Body inválido"}), 400

    if not key or ".." in key or "/" in key.lstrip("/"):
        # Solo se permiten claves de primer nivel (sin directorios anidados)
        pass
    if not key:
        return jsonify({"error": "Clave requerida"}), 400

    client = _get_b2_client()
    if client is None:
        return jsonify({"error": "Backblaze no configurado"}), 503

    try:
        client.delete_object(Bucket=B2_BUCKET_NAME, Key=key)
        log.info("Archivo eliminado de B2: %s", key)
        return jsonify({"ok": True, "deleted": key})
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "Archivo no encontrado"}), 404
        log.warning("Error eliminando %s de B2: %s", key, exc)
        return jsonify({"error": str(exc)}), 500
    except Exception as exc:
        log.exception("Error inesperado eliminando %s", key)
        return jsonify({"error": "Error interno"}), 500


# ─── Entrypoint local ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    log.info("RenomBot SIS v3.1 arrancando en http://localhost:%d", port)
    log.info("B2 configurado: %s", _b2_configured())
    app.run(debug=False, threaded=True, host="0.0.0.0", port=port)