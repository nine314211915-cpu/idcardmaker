from flask import Flask, request, jsonify, render_template, send_file, send_from_directory, session, redirect, url_for
import json, os, random, string, zipfile, io, tempfile, csv
from datetime import datetime
from functools import wraps
from PIL import Image, ImageOps, ImageEnhance, ImageFilter, UnidentifiedImageError
from werkzeug.utils import secure_filename

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME_DIR = tempfile.gettempdir() if os.environ.get("VERCEL") else BASE_DIR
UPLOAD_DIR = os.path.join(RUNTIME_DIR, "static", "uploads")
ASSET_DIR = os.path.join(RUNTIME_DIR, "static", "assets")
DATA_FILE = os.path.join(RUNTIME_DIR, "records.json")
CERTIFICATE_DATA_FILE = os.path.join(RUNTIME_DIR, "certificates.json")
SETTINGS_FILE = os.path.join(RUNTIME_DIR, "settings.json")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(ASSET_DIR, exist_ok=True)
app.config["SIGNATURE_UPLOAD_PASSWORD"] = os.environ.get("SIGNATURE_UPLOAD_PASSWORD", "admin123")
app.config["ADMIN_PANEL_PASSWORD"] = os.environ.get("ADMIN_PANEL_PASSWORD", "admin123")
app.config["GOOGLE_DRIVE_ROOT_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID", "").strip()
app.config["GOOGLE_DRIVE_PHOTOS_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_PHOTOS_FOLDER_ID", "").strip()
app.config["GOOGLE_DRIVE_BACKGROUNDS_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_BACKGROUNDS_FOLDER_ID", "").strip()
app.config["GOOGLE_DRIVE_SIGNATURES_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_SIGNATURES_FOLDER_ID", "").strip()
app.config["GOOGLE_DRIVE_DATA_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_DATA_FOLDER_ID", "").strip()

_drive_service = None
_drive_json_file_ids = {}
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "medical-id-card-secret")

for bundled_dir, runtime_dir in [
    (os.path.join(BASE_DIR, "static", "uploads"), UPLOAD_DIR),
    (os.path.join(BASE_DIR, "static", "assets"), ASSET_DIR),
]:
    if bundled_dir == runtime_dir or not os.path.isdir(bundled_dir):
        continue
    for filename in os.listdir(bundled_dir):
        src = os.path.join(bundled_dir, filename)
        dst = os.path.join(runtime_dir, filename)
        if os.path.isfile(src) and not os.path.exists(dst):
            with open(src, "rb") as src_file, open(dst, "wb") as dst_file:
                dst_file.write(src_file.read())

for bundled_file, runtime_file in [
    (os.path.join(BASE_DIR, "records.json"), DATA_FILE),
    (os.path.join(BASE_DIR, "certificates.json"), CERTIFICATE_DATA_FILE),
    (os.path.join(BASE_DIR, "settings.json"), SETTINGS_FILE),
]:
    if bundled_file == runtime_file or not os.path.isfile(bundled_file) or os.path.exists(runtime_file):
        continue
    with open(bundled_file, "rb") as src_file, open(runtime_file, "wb") as dst_file:
        dst_file.write(src_file.read())

def load_records():
    return load_json_store(DATA_FILE, "records.json", [])

def save_records(records):
    save_json_store(DATA_FILE, "records.json", records)


def load_certificates():
    return load_json_store(CERTIFICATE_DATA_FILE, "certificates.json", [])


def save_certificates(certificates):
    save_json_store(CERTIFICATE_DATA_FILE, "certificates.json", certificates)


def load_settings():
    defaults = {
        "background_url": "",
        "signature_url": "",
        "signature_drive_id": "",
        "backgrounds": {},
        "background_drive_ids": {},
        "certificate_backgrounds": {},
        "certificate_background_drive_ids": {},
    }
    loaded = load_json_store(SETTINGS_FILE, "settings.json", defaults)
    if isinstance(loaded, dict):
        defaults.update(loaded)
    if not isinstance(defaults.get("backgrounds"), dict):
        defaults["backgrounds"] = {}
    if not isinstance(defaults.get("background_drive_ids"), dict):
        defaults["background_drive_ids"] = {}
    if not isinstance(defaults.get("certificate_backgrounds"), dict):
        defaults["certificate_backgrounds"] = {}
    if not isinstance(defaults.get("certificate_background_drive_ids"), dict):
        defaults["certificate_background_drive_ids"] = {}
    return defaults


def save_settings(settings):
    save_json_store(SETTINGS_FILE, "settings.json", settings)


def normalize_date(value):
    value = (value or "").strip()
    if not value:
        return ""
    if "/" in value:
        parts = value.split("/")
        if len(parts) == 3:
            day, month, year = parts
            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
        return value
    if "-" in value:
        parts = value.split("-")
        if len(parts) == 3:
            if len(parts[0]) == 4:
                year, month, day = parts
                return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
            day, month, year = parts
            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
    return value


def current_date_display():
    return datetime.now().strftime("%d/%m/%Y")


def current_timestamp_display():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def normalize_record_dates(data):
    for field in ("dob", "valid_upto", "inserted_date", "issue_date"):
        if field in data:
            data[field] = normalize_date(data.get(field))
    return data


def canonicalize_institute_name(value):
    value = (value or "").strip()
    if value == "Govt. ANM Training, Jhunjhunu":
        return "Govt. ANM Training Center, Jhunjhunu"
    return value


def load_drive_service_account_info():
    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_json:
        return json.loads(raw_json)

    file_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    if file_path and os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def is_drive_enabled():
    return load_drive_service_account_info() is not None


def get_drive_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service

    service_account_info = load_drive_service_account_info()
    if not service_account_info:
        return None

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    _drive_service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    return _drive_service


def get_drive_folder_id(kind):
    return (
        app.config.get(f"GOOGLE_DRIVE_{kind.upper()}_FOLDER_ID")
        or app.config["GOOGLE_DRIVE_ROOT_FOLDER_ID"]
        or None
    )


def make_drive_public(service, file_id):
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        fields="id",
    ).execute()


def build_drive_view_url(file_id):
    return f"https://drive.google.com/uc?export=view&id={file_id}"


def upload_bytes_to_drive(file_bytes, filename, mime_type, kind):
    service = get_drive_service()
    if service is None:
        return None, None

    from googleapiclient.http import MediaIoBaseUpload

    metadata = {"name": filename}
    folder_id = get_drive_folder_id(kind)
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=False)
    created = service.files().create(body=metadata, media_body=media, fields="id").execute()
    file_id = created["id"]
    make_drive_public(service, file_id)
    return file_id, build_drive_view_url(file_id)


def _drive_query_string(value):
    return value.replace("\\", "\\\\").replace("'", "\\'")


def find_drive_file_id(filename, kind):
    service = get_drive_service()
    if service is None:
        return None

    folder_id = get_drive_folder_id(kind)
    query = [f"name='{_drive_query_string(filename)}'", "trashed=false"]
    if folder_id:
        query.append(f"'{_drive_query_string(folder_id)}' in parents")
    response = service.files().list(
        q=" and ".join(query),
        fields="files(id, name)",
        pageSize=1,
    ).execute()
    files = response.get("files", [])
    return files[0]["id"] if files else None


def download_drive_file_bytes(file_id):
    service = get_drive_service()
    if service is None or not file_id:
        return None

    from googleapiclient.http import MediaIoBaseDownload

    buffer = io.BytesIO()
    request = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def upsert_private_drive_file(file_bytes, filename, mime_type, kind):
    service = get_drive_service()
    if service is None:
        return None

    from googleapiclient.http import MediaIoBaseUpload

    file_id = _drive_json_file_ids.get(filename) or find_drive_file_id(filename, kind)
    metadata = {"name": filename}
    folder_id = get_drive_folder_id(kind)
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=False)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        _drive_json_file_ids[filename] = file_id
        return file_id

    created = service.files().create(body=metadata, media_body=media, fields="id").execute()
    file_id = created["id"]
    _drive_json_file_ids[filename] = file_id
    return file_id


def load_json_store(local_path, drive_filename, default_value):
    if is_drive_enabled():
        try:
            file_id = _drive_json_file_ids.get(drive_filename) or find_drive_file_id(drive_filename, "data")
            if file_id:
                _drive_json_file_ids[drive_filename] = file_id
                file_bytes = download_drive_file_bytes(file_id)
                if file_bytes is not None:
                    with open(local_path, "wb") as f:
                        f.write(file_bytes)
                    return json.loads(file_bytes.decode("utf-8"))
            elif os.path.exists(local_path):
                with open(local_path, "rb") as f:
                    existing_bytes = f.read()
                if existing_bytes:
                    upsert_private_drive_file(existing_bytes, drive_filename, "application/json", "data")
                    return json.loads(existing_bytes.decode("utf-8"))
        except Exception:
            app.logger.warning("Drive sync failed while loading %s", drive_filename, exc_info=True)

    if os.path.exists(local_path):
        with open(local_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass

    return json.loads(json.dumps(default_value))


def save_json_store(local_path, drive_filename, payload):
    json_text = json.dumps(payload, indent=2)
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(json_text)

    if is_drive_enabled():
        try:
            upsert_private_drive_file(json_text.encode("utf-8"), drive_filename, "application/json", "data")
        except Exception:
            app.logger.warning("Drive sync failed while saving %s", drive_filename, exc_info=True)


def build_drive_storage_status():
    data_folder_id = get_drive_folder_id("data")
    files = []
    for filename in ("records.json", "certificates.json", "settings.json"):
        file_id = None
        try:
            file_id = _drive_json_file_ids.get(filename) or find_drive_file_id(filename, "data")
            if file_id:
                _drive_json_file_ids[filename] = file_id
        except Exception:
            app.logger.warning("Unable to resolve Drive file status for %s", filename, exc_info=True)
        files.append({
            "name": filename,
            "present": bool(file_id),
            "file_id": file_id or "",
        })

    return {
        "drive_enabled": is_drive_enabled(),
        "data_folder_id": data_folder_id or "",
        "files": files,
    }


def delete_drive_file(file_id):
    if not file_id:
        return
    service = get_drive_service()
    if service is None:
        return
    try:
        service.files().delete(fileId=file_id).execute()
    except Exception:
        pass


def delete_local_file_if_exists(path):
    if path and os.path.exists(path):
        os.remove(path)


def delete_uploaded_file_from_url(file_url):
    if not file_url:
        return
    marker = "/uploads/"
    if marker not in file_url:
        return
    filename = file_url.split(marker, 1)[1].split("?", 1)[0].strip("/")
    if filename:
        delete_local_file_if_exists(os.path.join(UPLOAD_DIR, filename))


def download_drive_file(file_id):
    if not file_id:
        return None
    service = get_drive_service()
    if service is None:
        return None

    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def admin_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("admin_authenticated"):
            return jsonify({"error": "Unauthorized"}), 401
        return view_func(*args, **kwargs)
    return wrapped


def make_asset_slug(value):
    safe = secure_filename((value or "").strip())
    return safe or "default"


def get_filtered_records():
    records = load_records()
    institute = canonicalize_institute_name(request.args.get("institute"))
    if institute:
        records = [r for r in records if canonicalize_institute_name(r.get("institute_name")) == institute]
    return records, institute


def summarize_batch(records):
    saved = [r for r in records if not r.get("submitted_at")]
    submitted = [r for r in records if r.get("submitted_at")]
    return {
        "saved_count": len(saved),
        "submitted_count": len(submitted),
        "total_count": len(records),
    }

def gen_serial():
    date_part = datetime.now().strftime("%Y%m%d")
    rand_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"ID-{date_part}-{rand_part}"


def gen_certificate_no():
    date_part = datetime.now().strftime("%Y%m%d")
    rand_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    return f"CERT-{date_part}-{rand_part}"

def detect_primary_face(img):
    try:
        import cv2
        import numpy as np
    except Exception:
        return None

    rgb = np.array(img)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    cascade_path = os.path.join(cv2.data.haarcascades, "haarcascade_frontalface_default.xml")
    face_cascade = cv2.CascadeClassifier(cascade_path)
    faces = face_cascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(60, 60),
    )
    if len(faces) == 0:
        return None
    return max(faces, key=lambda face: face[2] * face[3])


def crop_passport_frame(img, face_box=None):
    w, h = img.size
    target_ratio = 3 / 4

    if face_box is not None:
        fx, fy, fw, fh = [int(v) for v in face_box]
        crop_h = min(h, max(int(fh * 3.2), 220))
        crop_w = int(crop_h * target_ratio)

        if crop_w > w:
            crop_w = w
            crop_h = int(crop_w / target_ratio)

        center_x = fx + fw // 2
        face_center_y = fy + fh // 2
        center_y = int(face_center_y + fh * 0.55)

        left = max(0, min(center_x - crop_w // 2, w - crop_w))
        top = max(0, min(center_y - crop_h // 2, h - crop_h))
        return img.crop((left, top, left + crop_w, top + crop_h))

    current_ratio = w / h
    if current_ratio > target_ratio:
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        return img.crop((left, 0, left + new_w, h))

    new_h = int(w / target_ratio)
    top = max(0, int((h - new_h) * 0.16))
    return img.crop((0, top, w, min(h, top + new_h)))


def autocrop_passport(img_path, out_path):
    """Create a 3:4 passport crop that prioritizes the largest detected face."""
    img = Image.open(img_path)
    img = ImageOps.exif_transpose(img).convert("RGB")
    face_box = detect_primary_face(img)
    img = crop_passport_frame(img, face_box)
    img = img.resize((300, 400), Image.LANCZOS)
    img = ImageEnhance.Brightness(img).enhance(1.03)
    img = ImageEnhance.Contrast(img).enhance(1.08)
    img = ImageEnhance.Color(img).enhance(1.03)
    img = ImageEnhance.Sharpness(img).enhance(1.18)
    img = img.filter(ImageFilter.UnsharpMask(radius=1.4, percent=120, threshold=2))
    img.save(out_path, "JPEG", quality=92)


def save_image_asset(file_storage, filename, kind):
    try:
        file_storage.stream.seek(0)
    except Exception:
        pass

    file_bytes = file_storage.read()
    if not file_bytes:
        raise ValueError("Empty image upload")

    try:
        img = Image.open(io.BytesIO(file_bytes))
        img = ImageOps.exif_transpose(img).convert("RGB")
    except UnidentifiedImageError as exc:
        raise ValueError("Unsupported image format") from exc

    local_path = os.path.join(ASSET_DIR, filename)
    output = io.BytesIO()
    img.save(output, "JPEG", quality=92)
    output_bytes = output.getvalue()
    with open(local_path, "wb") as image_file:
        image_file.write(output_bytes)

    drive_id = None
    drive_url = None
    if is_drive_enabled():
        try:
            drive_id, drive_url = upload_bytes_to_drive(output_bytes, filename, "image/jpeg", kind)
        except Exception:
            app.logger.warning("Drive upload failed for asset %s", filename, exc_info=True)
    return drive_url or f"/generated-assets/{filename}", drive_id


def save_background_image(file_storage, institute_name):
    filename = f"card_background_{make_asset_slug(institute_name)}.jpg"
    return save_image_asset(file_storage, filename, "backgrounds")


def save_certificate_background_image(file_storage, institute_name):
    filename = f"certificate_background_{make_asset_slug(institute_name)}.jpg"
    return save_image_asset(file_storage, filename, "backgrounds")


def save_signature_image(file_storage):
    img = Image.open(file_storage.stream)
    img = ImageOps.exif_transpose(img)
    if img.mode not in ("RGBA", "LA"):
        img = img.convert("RGBA")
    img = img.convert("RGBA")
    pixels = []
    for r, g, b, a in img.getdata():
        if a == 0:
            pixels.append((r, g, b, a))
            continue
        brightness = (r + g + b) / 3
        channel_spread = max(r, g, b) - min(r, g, b)
        if brightness > 235 and channel_spread < 22:
            pixels.append((255, 255, 255, 0))
            continue
        ink_boost = max(0, int((235 - brightness) * 1.35))
        alpha = max(70, min(255, ink_boost + 70))
        pixels.append((20, 20, 20, alpha))
    img.putdata(pixels)
    bbox = img.getbbox()
    if bbox:
        img = img.crop(bbox)
    local_path = os.path.join(ASSET_DIR, "hod_signature.png")
    img.save(local_path, "PNG")
    drive_id = None
    drive_url = None
    if is_drive_enabled():
        with open(local_path, "rb") as f:
            drive_id, drive_url = upload_bytes_to_drive(f.read(), "hod_signature.png", "image/png", "signatures")
    return drive_url or "/generated-assets/hod_signature.png", drive_id

@app.route("/")
def home():
    return render_template("home.html")


@app.route("/id-card")
def index():
    return render_template("index.html")


@app.route("/certificate")
def certificate():
    return render_template("certificate.html")

@app.route("/admin")
def admin():
    if not session.get("admin_authenticated"):
        return render_template("admin_login.html")
    return render_template("admin.html")


@app.route("/admin/login", methods=["POST"])
def admin_login():
    password = request.form.get("password", "")
    if password == app.config["ADMIN_PANEL_PASSWORD"]:
        session["admin_authenticated"] = True
        return redirect(url_for("admin"))
    return render_template("admin_login.html", error="Incorrect admin password"), 401


@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    session.pop("admin_authenticated", None)
    return redirect(url_for("admin"))


@app.route("/api/settings")
def get_settings():
    settings = load_settings()
    institute = canonicalize_institute_name(request.args.get("institute"))
    background_url = settings.get("backgrounds", {}).get(institute, "") if institute else ""
    return jsonify({
        "background_url": background_url,
        "signature_url": settings.get("signature_url", ""),
        "institute": institute
    })


@app.route("/api/certificate-settings")
def get_certificate_settings():
    settings = load_settings()
    institute = canonicalize_institute_name(request.args.get("institute"))
    background_url = settings.get("certificate_backgrounds", {}).get(institute, "") if institute else ""
    return jsonify({
        "background_url": background_url,
        "institute": institute
    })


@app.route("/api/drive-storage-status")
@admin_required
def drive_storage_status():
    return jsonify(build_drive_storage_status())

@app.route("/uploads/<filename>")
def uploaded_file(filename):
    return send_from_directory(UPLOAD_DIR, filename)

@app.route("/generated-assets/<filename>")
def generated_asset(filename):
    return send_from_directory(ASSET_DIR, filename)

@app.route("/api/upload-photo", methods=["POST"])
def upload_photo():
    if "photo" not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files["photo"]
    serial = (request.form.get("serial_no") or "").strip() or gen_serial()
    filename = f"{serial}.jpg"
    raw_path = os.path.join(UPLOAD_DIR, f"raw_{filename}")
    final_path = os.path.join(UPLOAD_DIR, filename)
    f.save(raw_path)
    try:
        autocrop_passport(raw_path, final_path)
        os.remove(raw_path)
    except Exception:
        os.rename(raw_path, final_path)
    photo_url = f"/uploads/{filename}"
    photo_drive_id = None
    if is_drive_enabled():
        try:
            with open(final_path, "rb") as photo_file:
                photo_drive_id, drive_url = upload_bytes_to_drive(photo_file.read(), filename, "image/jpeg", "photos")
                if drive_url:
                    photo_url = drive_url
        except Exception:
            photo_drive_id = None
    return jsonify({"serial_no": serial, "photo_url": photo_url, "photo_drive_id": photo_drive_id})


def attach_photo_to_serial(file_storage, serial):
    filename = f"{serial}.jpg"
    raw_path = os.path.join(UPLOAD_DIR, f"raw_{filename}")
    final_path = os.path.join(UPLOAD_DIR, filename)
    file_storage.save(raw_path)
    try:
        autocrop_passport(raw_path, final_path)
        os.remove(raw_path)
    except Exception:
        os.rename(raw_path, final_path)
    photo_url = f"/uploads/{filename}"
    photo_drive_id = None
    if is_drive_enabled():
        with open(final_path, "rb") as photo_file:
            photo_drive_id, drive_url = upload_bytes_to_drive(photo_file.read(), filename, "image/jpeg", "photos")
            if drive_url:
                photo_url = drive_url
    return photo_url, photo_drive_id


@app.route("/api/upload-background", methods=["POST"])
@admin_required
def upload_background():
    if "background" not in request.files:
        return jsonify({"error": "No file"}), 400
    institute = canonicalize_institute_name(request.form.get("institute"))
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    file_storage = request.files["background"]
    if not secure_filename(file_storage.filename):
        return jsonify({"error": "Invalid filename"}), 400
    try:
        background_url, background_drive_id = save_background_image(file_storage, institute)
    except Exception:
        return jsonify({"error": "Unable to process background image"}), 400
    settings = load_settings()
    old_drive_id = settings.setdefault("background_drive_ids", {}).get(institute)
    if old_drive_id and old_drive_id != background_drive_id:
        delete_drive_file(old_drive_id)
    settings.setdefault("backgrounds", {})[institute] = background_url
    if background_drive_id:
        settings.setdefault("background_drive_ids", {})[institute] = background_drive_id
    if settings.get("background_url") and not settings["backgrounds"].get("default"):
        settings["backgrounds"]["default"] = settings["background_url"]
    save_settings(settings)
    return jsonify({"status": "saved", "background_url": background_url, "institute": institute})


@app.route("/api/upload-certificate-background", methods=["POST"])
@admin_required
def upload_certificate_background():
    if "background" not in request.files:
        return jsonify({"error": "No file"}), 400
    institute = canonicalize_institute_name(request.form.get("institute"))
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    file_storage = request.files["background"]
    if not secure_filename(file_storage.filename):
        return jsonify({"error": "Invalid filename"}), 400
    try:
        background_url, background_drive_id = save_certificate_background_image(file_storage, institute)
    except Exception:
        return jsonify({"error": "Unable to process certificate background image"}), 400

    settings = load_settings()
    old_drive_id = settings.setdefault("certificate_background_drive_ids", {}).get(institute)
    if old_drive_id and old_drive_id != background_drive_id:
        delete_drive_file(old_drive_id)
    settings.setdefault("certificate_backgrounds", {})[institute] = background_url
    if background_drive_id:
        settings.setdefault("certificate_background_drive_ids", {})[institute] = background_drive_id
    save_settings(settings)
    return jsonify({"status": "saved", "background_url": background_url, "institute": institute})


@app.route("/api/upload-signature", methods=["POST"])
def upload_signature():
    if request.form.get("password", "") != app.config["SIGNATURE_UPLOAD_PASSWORD"]:
        return jsonify({"error": "Incorrect password"}), 403
    if "signature" not in request.files:
        return jsonify({"error": "No file"}), 400
    file_storage = request.files["signature"]
    if not secure_filename(file_storage.filename):
        return jsonify({"error": "Invalid filename"}), 400
    try:
        signature_url, signature_drive_id = save_signature_image(file_storage)
    except Exception:
        return jsonify({"error": "Unable to process signature image"}), 400
    settings = load_settings()
    old_signature_drive_id = settings.get("signature_drive_id")
    if old_signature_drive_id and old_signature_drive_id != signature_drive_id:
        delete_drive_file(old_signature_drive_id)
    settings["signature_url"] = signature_url
    settings["signature_drive_id"] = signature_drive_id or ""
    save_settings(settings)
    return jsonify({"status": "saved", "signature_url": signature_url})


@app.route("/api/delete-background", methods=["DELETE"])
@admin_required
def delete_background():
    institute = canonicalize_institute_name(request.args.get("institute"))
    if not institute:
        return jsonify({"error": "Institute is required"}), 400

    settings = load_settings()
    old_drive_id = settings.setdefault("background_drive_ids", {}).pop(institute, None)
    old_url = settings.setdefault("backgrounds", {}).pop(institute, "")
    if old_drive_id:
        delete_drive_file(old_drive_id)
    delete_local_file_if_exists(os.path.join(ASSET_DIR, f"card_background_{make_asset_slug(institute)}.jpg"))
    save_settings(settings)
    return jsonify({"status": "deleted", "background_url": old_url, "institute": institute})


@app.route("/api/delete-certificate-background", methods=["DELETE"])
@admin_required
def delete_certificate_background():
    institute = canonicalize_institute_name(request.args.get("institute"))
    if not institute:
        return jsonify({"error": "Institute is required"}), 400

    settings = load_settings()
    old_drive_id = settings.setdefault("certificate_background_drive_ids", {}).pop(institute, None)
    old_url = settings.setdefault("certificate_backgrounds", {}).pop(institute, "")
    if old_drive_id:
        delete_drive_file(old_drive_id)
    delete_local_file_if_exists(os.path.join(ASSET_DIR, f"certificate_background_{make_asset_slug(institute)}.jpg"))
    save_settings(settings)
    return jsonify({"status": "deleted", "background_url": old_url, "institute": institute})


@app.route("/api/delete-signature", methods=["DELETE"])
@admin_required
def delete_signature():
    settings = load_settings()
    old_drive_id = settings.get("signature_drive_id")
    if old_drive_id:
        delete_drive_file(old_drive_id)
    settings["signature_url"] = ""
    settings["signature_drive_id"] = ""
    delete_local_file_if_exists(os.path.join(ASSET_DIR, "hod_signature.png"))
    save_settings(settings)
    return jsonify({"status": "deleted"})

@app.route("/api/submit", methods=["POST"])
def submit():
    data = request.json
    data["institute_name"] = canonicalize_institute_name(data.get("institute_name"))
    normalize_record_dates(data)
    data["saved_at"] = current_timestamp_display()
    records = load_records()
    # Check for duplicate serial
    for r in records:
        if r.get("serial_no") == data.get("serial_no"):
            submitted_at = r.get("submitted_at")
            batch_total_cards = r.get("batch_total_cards")
            r.update(data)
            if submitted_at:
                r["submitted_at"] = submitted_at
            if batch_total_cards:
                r["batch_total_cards"] = batch_total_cards
            save_records(records)
            return jsonify({"status": "updated"})
    data["submitted_at"] = None
    records.append(data)
    save_records(records)
    return jsonify({"status": "saved"})


@app.route("/api/certificates", methods=["GET", "POST"])
def certificates_api():
    if request.method == "POST":
        data = request.json or {}
        if not (data.get("recipient_name") or "").strip():
            return jsonify({"error": "Recipient name is required"}), 400
        institute_name = (data.get("institute_name") or "").strip()
        if not institute_name:
            return jsonify({"error": "Institute is required"}), 400
        data["institute_name"] = canonicalize_institute_name(institute_name)

        certificates = load_certificates()
        certificate_no = (data.get("certificate_no") or "").strip() or gen_certificate_no()
        data["certificate_no"] = certificate_no
        normalize_record_dates(data)
        data["saved_at"] = current_timestamp_display()

        for certificate in certificates:
            if certificate.get("certificate_no") == certificate_no:
                certificate.update(data)
                save_certificates(certificates)
                return jsonify({"status": "updated", "certificate_no": certificate_no})

        certificates.append(data)
        save_certificates(certificates)
        return jsonify({"status": "saved", "certificate_no": certificate_no})

    certificates = load_certificates()
    institute = canonicalize_institute_name(request.args.get("institute"))
    if institute:
        certificates = [c for c in certificates if canonicalize_institute_name(c.get("institute_name")) == institute]
    return jsonify({"certificates": certificates, "institute": institute})


@app.route("/api/certificates/<certificate_no>", methods=["DELETE"])
@admin_required
def delete_certificate(certificate_no):
    certificates = load_certificates()
    certificate_to_delete = next((c for c in certificates if c.get("certificate_no") == certificate_no), None)
    certificates = [c for c in certificates if c.get("certificate_no") != certificate_no]
    save_certificates(certificates)

    if certificate_to_delete:
        if certificate_to_delete.get("photo_drive_id"):
            delete_drive_file(certificate_to_delete.get("photo_drive_id"))
        delete_uploaded_file_from_url(certificate_to_delete.get("photo_url"))

    return jsonify({"status": "deleted", "certificate_no": certificate_no})


@app.route("/api/batch-summary")
def batch_summary():
    records, institute = get_filtered_records()
    summary = summarize_batch(records)
    summary["institute"] = institute
    return jsonify(summary)


@app.route("/api/batch-records")
def batch_records():
    records, institute = get_filtered_records()
    return jsonify({"records": records, "institute": institute})


@app.route("/api/submit-batch", methods=["POST"])
def submit_batch():
    payload = request.json or {}
    institute = canonicalize_institute_name(payload.get("institute_name"))
    if not institute:
        return jsonify({"error": "Institute is required"}), 400

    records = load_records()
    saved_records = [r for r in records if (r.get("institute_name") or "").strip() == institute and not r.get("submitted_at")]
    if not saved_records:
        return jsonify({"error": "No saved ID cards are pending submission for this institute"}), 400

    submitted_at = current_timestamp_display()
    for rec in records:
        if (rec.get("institute_name") or "").strip() == institute and not rec.get("submitted_at"):
            rec["submitted_at"] = submitted_at
            rec["batch_total_cards"] = len(saved_records)
    save_records(records)
    return jsonify({
        "status": "submitted",
        "submitted_at": submitted_at,
        "total_cards": len(saved_records),
        "institute_name": institute
    })

@app.route("/api/records")
@admin_required
def get_records():
    records, institute = get_filtered_records()
    return jsonify({"records": records, "institute": institute})


@app.route("/api/admin-attach-photo", methods=["POST"])
@admin_required
def admin_attach_photo():
    serial = (request.form.get("serial_no") or "").strip()
    if not serial:
        return jsonify({"error": "Serial number is required"}), 400
    if "photo" not in request.files:
        return jsonify({"error": "No photo file"}), 400

    records = load_records()
    target_record = next((rec for rec in records if rec.get("serial_no") == serial), None)
    if not target_record:
        return jsonify({"error": "Record not found"}), 404

    old_drive_id = target_record.get("photo_drive_id")
    try:
        photo_url, photo_drive_id = attach_photo_to_serial(request.files["photo"], serial)
    except Exception:
        return jsonify({"error": "Unable to process photo"}), 400

    if old_drive_id and old_drive_id != photo_drive_id:
        delete_drive_file(old_drive_id)

    target_record["photo_url"] = photo_url
    target_record["photo_drive_id"] = photo_drive_id or ""
    save_records(records)
    return jsonify({"status": "saved", "serial_no": serial, "photo_url": photo_url})


@app.route("/api/import-csv", methods=["POST"])
@admin_required
def import_csv():
    if "csv_file" not in request.files:
        return jsonify({"error": "No CSV file"}), 400

    uploaded = request.files["csv_file"]
    if not secure_filename(uploaded.filename):
        return jsonify({"error": "Invalid CSV filename"}), 400

    try:
        text = uploaded.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return jsonify({"error": "CSV must be UTF-8 encoded"}), 400

    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return jsonify({"error": "CSV has no data rows"}), 400

    records = load_records()
    record_map = {rec.get("serial_no"): rec for rec in records if rec.get("serial_no")}
    imported = 0
    updated = 0

    field_map = {
        "serial_no": ["serial_no", "serial", "id"],
        "institute_name": ["institute_name", "institute"],
        "profile_type": ["profile_type", "type"],
        "name": ["name", "full_name"],
        "course": ["course"],
        "training_year": ["training_year", "year"],
        "batch_session": ["batch_session", "batch"],
        "father_name": ["father_name", "fathers_name", "father"],
        "aadhaar_no": ["aadhaar_no", "aadhaar", "aadhar_no", "aadhar"],
        "employee_id": ["employee_id", "emp_id"],
        "designation": ["designation"],
        "department": ["department", "dept", "section"],
        "dob": ["dob", "date_of_birth"],
        "contact": ["contact", "contact_no", "mobile"],
        "blood_group": ["blood_group", "blood"],
        "address": ["address"],
        "valid_upto": ["valid_upto", "validity"],
        "inserted_date": ["inserted_date", "issue_date", "issued_date"],
    }

    for row in rows:
        normalized = {}
        for target, aliases in field_map.items():
            value = ""
            for alias in aliases:
                if alias in row and str(row.get(alias) or "").strip():
                    value = str(row.get(alias) or "").strip()
                    break
            if value:
                normalized[target] = value

        normalized["institute_name"] = canonicalize_institute_name(normalized.get("institute_name"))
        normalize_record_dates(normalized)
        serial_no = (normalized.get("serial_no") or "").strip() or gen_serial()
        normalized["serial_no"] = serial_no
        if not normalized.get("name"):
            continue

        profile_type = (normalized.get("profile_type") or "").strip().lower()
        if profile_type not in {"student", "lecturer", "employee"}:
            profile_type = "student" if normalized.get("course") or normalized.get("training_year") or normalized.get("batch_session") else "employee"
        normalized["profile_type"] = profile_type

        existing = record_map.get(serial_no)
        if existing:
            preserved = {
                "submitted_at": existing.get("submitted_at"),
                "batch_total_cards": existing.get("batch_total_cards"),
                "photo_url": existing.get("photo_url", ""),
                "photo_drive_id": existing.get("photo_drive_id", ""),
            }
            existing.update(normalized)
            existing.update({k: v for k, v in preserved.items() if v is not None})
            existing["saved_at"] = current_timestamp_display()
            updated += 1
        else:
            normalized.setdefault("photo_url", "")
            normalized.setdefault("photo_drive_id", "")
            normalized["saved_at"] = current_timestamp_display()
            normalized["submitted_at"] = None
            records.append(normalized)
            record_map[serial_no] = normalized
            imported += 1

    save_records(records)
    return jsonify({"status": "ok", "imported": imported, "updated": updated, "total": imported + updated})

@app.route("/api/delete/<serial_no>", methods=["DELETE"])
@admin_required
def delete_record(serial_no):
    records = load_records()
    record_to_delete = next((r for r in records if r.get("serial_no") == serial_no), None)
    records = [r for r in records if r.get("serial_no") != serial_no]
    save_records(records)
    if record_to_delete and record_to_delete.get("photo_drive_id"):
        delete_drive_file(record_to_delete.get("photo_drive_id"))
    delete_uploaded_file_from_url((record_to_delete or {}).get("photo_url"))
    delete_local_file_if_exists(os.path.join(UPLOAD_DIR, f"{serial_no}.jpg"))
    return jsonify({"status": "deleted"})

@app.route("/api/export-excel")
@admin_required
def export_excel():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    records, institute_filter = get_filtered_records()
    wb = Workbook()
    ws = wb.active
    ws.title = "ID Card Records"
    headers = ["Serial No", "Profile Type", "Name", "Course/Designation", "Batch", "Father Name", "Aadhaar No.",
               "Employee ID", "Department", "Date of Birth", "Contact No", "Blood Group", "Address", "Valid Upto",
               "Institute", "Photo File", "Saved At"]
    header_fill = PatternFill(start_color="8B0000", end_color="8B0000", fill_type="solid")
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
    for row, rec in enumerate(records, 2):
        ws.cell(row=row, column=1, value=rec.get("serial_no", ""))
        ws.cell(row=row, column=2, value=rec.get("profile_type", ""))
        ws.cell(row=row, column=3, value=rec.get("name", ""))
        ws.cell(row=row, column=4, value=rec.get("training_year") or rec.get("course") or rec.get("designation", ""))
        ws.cell(row=row, column=5, value=rec.get("batch_session", ""))
        ws.cell(row=row, column=6, value=rec.get("father_name", ""))
        ws.cell(row=row, column=7, value=rec.get("aadhaar_no", ""))
        ws.cell(row=row, column=8, value=rec.get("employee_id", ""))
        ws.cell(row=row, column=9, value=rec.get("department", ""))
        ws.cell(row=row, column=10, value=rec.get("dob", ""))
        ws.cell(row=row, column=11, value=rec.get("contact", ""))
        ws.cell(row=row, column=12, value=rec.get("blood_group", ""))
        ws.cell(row=row, column=13, value=rec.get("address", ""))
        ws.cell(row=row, column=14, value=rec.get("valid_upto", ""))
        ws.cell(row=row, column=15, value=rec.get("institute_name", ""))
        ws.cell(row=row, column=16, value=f"{rec.get('serial_no', '')}.jpg")
        ws.cell(row=row, column=17, value=rec.get("saved_at", ""))
    for col in ws.columns:
        max_len = max((len(str(c.value or "")) for c in col), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    institute = institute_filter or (records[0].get("institute_name", "IDCardRecords") if records else "IDCardRecords")
    institute_safe = "".join(c for c in institute if c.isalnum() or c in "_ -")
    return send_file(buf, as_attachment=True,
                     download_name=f"{institute_safe}_IDCards_{datetime.now().strftime('%Y%m%d')}.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/api/export-zip")
@admin_required
def export_zip():
    records, institute_filter = get_filtered_records()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow([
            "Serial No", "Profile Type", "Name", "Course/Designation", "Batch", "Father Name", "Aadhaar No.",
            "Employee ID", "Department", "Date of Birth", "Contact No", "Blood Group", "Address", "Valid Upto",
            "Institute", "Photo File", "Photo URL", "Saved At", "Submitted At"
        ])
        for rec in records:
            serial = rec.get("serial_no", "")
            writer.writerow([
                rec.get("serial_no", ""),
                rec.get("profile_type", ""),
                rec.get("name", ""),
                rec.get("training_year") or rec.get("course") or rec.get("designation", ""),
                rec.get("batch_session", ""),
                rec.get("father_name", ""),
                rec.get("aadhaar_no", ""),
                rec.get("employee_id", ""),
                rec.get("department", ""),
                rec.get("dob", ""),
                rec.get("contact", ""),
                rec.get("blood_group", ""),
                rec.get("address", ""),
                rec.get("valid_upto", ""),
                rec.get("institute_name", ""),
                f"{serial}.jpg" if serial else "",
                rec.get("photo_url", ""),
                rec.get("saved_at", ""),
                rec.get("submitted_at", ""),
            ])
            photo_path = os.path.join(UPLOAD_DIR, f"{serial}.jpg")
            if os.path.exists(photo_path):
                zf.write(photo_path, f"photos/{serial}.jpg")
            elif rec.get("photo_drive_id"):
                photo_bytes = download_drive_file(rec.get("photo_drive_id"))
                if photo_bytes:
                    zf.writestr(f"photos/{serial}.jpg", photo_bytes)
        zf.writestr("records.csv", csv_buf.getvalue())
    buf.seek(0)
    institute = institute_filter or (records[0].get("institute_name", "IDCardRecords") if records else "IDCardRecords")
    institute_safe = "".join(c for c in institute if c.isalnum() or c in "_ -")
    return send_file(buf, as_attachment=True,
                     download_name=f"{institute_safe}_IDCards_Export_{datetime.now().strftime('%Y%m%d')}.zip",
                     mimetype="application/zip")

if __name__ == "__main__":
    app.run(debug=True, port=5050)
