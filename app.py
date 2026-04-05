from flask import Flask, request, jsonify, render_template, send_file, send_from_directory
import json, os, random, string, zipfile, io, tempfile
from datetime import datetime
from PIL import Image, ImageOps
from werkzeug.utils import secure_filename

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME_DIR = tempfile.gettempdir() if os.environ.get("VERCEL") else BASE_DIR
UPLOAD_DIR = os.path.join(RUNTIME_DIR, "static", "uploads")
ASSET_DIR = os.path.join(RUNTIME_DIR, "static", "assets")
DATA_FILE = os.path.join(RUNTIME_DIR, "records.json")
SETTINGS_FILE = os.path.join(RUNTIME_DIR, "settings.json")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(ASSET_DIR, exist_ok=True)
app.config["SIGNATURE_UPLOAD_PASSWORD"] = os.environ.get("SIGNATURE_UPLOAD_PASSWORD", "admin123")

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
    (os.path.join(BASE_DIR, "settings.json"), SETTINGS_FILE),
]:
    if bundled_file == runtime_file or not os.path.isfile(bundled_file) or os.path.exists(runtime_file):
        continue
    with open(bundled_file, "rb") as src_file, open(runtime_file, "wb") as dst_file:
        dst_file.write(src_file.read())

def load_records():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []

def save_records(records):
    with open(DATA_FILE, "w") as f:
        json.dump(records, f, indent=2)


def load_settings():
    defaults = {"background_url": "", "signature_url": "", "backgrounds": {}}
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            try:
                defaults.update(json.load(f))
            except json.JSONDecodeError:
                pass
    if not isinstance(defaults.get("backgrounds"), dict):
        defaults["backgrounds"] = {}
    return defaults


def save_settings(settings):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)


def make_asset_slug(value):
    safe = secure_filename((value or "").strip())
    return safe or "default"


def get_filtered_records():
    records = load_records()
    institute = (request.args.get("institute") or "").strip()
    if institute:
        records = [r for r in records if (r.get("institute_name") or "").strip() == institute]
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

def autocrop_passport(img_path, out_path):
    """Crop image to passport style (3:4 ratio), face-centered best effort."""
    img = Image.open(img_path).convert("RGB")
    w, h = img.size
    target_ratio = 3 / 4
    current_ratio = w / h
    if current_ratio > target_ratio:
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        img = img.crop((left, 0, left + new_w, h))
    else:
        new_h = int(w / target_ratio)
        top = max(0, int((h - new_h) * 0.2))
        img = img.crop((0, top, w, top + new_h))
    img = img.resize((300, 400), Image.LANCZOS)
    img = ImageOps.exif_transpose(img)
    img.save(out_path, "JPEG", quality=90)


def save_background_image(file_storage, institute_name):
    img = Image.open(file_storage.stream)
    img = ImageOps.exif_transpose(img).convert("RGB")
    filename = f"card_background_{make_asset_slug(institute_name)}.jpg"
    img.save(os.path.join(ASSET_DIR, filename), "JPEG", quality=92)
    return f"/generated-assets/{filename}"


def save_signature_image(file_storage):
    img = Image.open(file_storage.stream)
    img = ImageOps.exif_transpose(img)
    if img.mode not in ("RGBA", "LA"):
        img = img.convert("RGBA")
    img.save(os.path.join(ASSET_DIR, "hod_signature.png"), "PNG")
    return "/generated-assets/hod_signature.png"

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/admin")
def admin():
    return render_template("admin.html")


@app.route("/api/settings")
def get_settings():
    settings = load_settings()
    institute = (request.args.get("institute") or "").strip()
    background_url = settings.get("backgrounds", {}).get(institute, "") if institute else ""
    return jsonify({
        "background_url": background_url,
        "signature_url": settings.get("signature_url", ""),
        "institute": institute
    })

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
    serial = gen_serial()
    filename = f"{serial}.jpg"
    raw_path = os.path.join(UPLOAD_DIR, f"raw_{filename}")
    final_path = os.path.join(UPLOAD_DIR, filename)
    f.save(raw_path)
    try:
        autocrop_passport(raw_path, final_path)
        os.remove(raw_path)
    except Exception as e:
        os.rename(raw_path, final_path)
    return jsonify({"serial_no": serial, "photo_url": f"/uploads/{filename}"})


@app.route("/api/upload-background", methods=["POST"])
def upload_background():
    if "background" not in request.files:
        return jsonify({"error": "No file"}), 400
    institute = (request.form.get("institute") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400
    file_storage = request.files["background"]
    if not secure_filename(file_storage.filename):
        return jsonify({"error": "Invalid filename"}), 400
    try:
        background_url = save_background_image(file_storage, institute)
    except Exception:
        return jsonify({"error": "Unable to process background image"}), 400
    settings = load_settings()
    settings.setdefault("backgrounds", {})[institute] = background_url
    if settings.get("background_url") and not settings["backgrounds"].get("default"):
        settings["backgrounds"]["default"] = settings["background_url"]
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
        signature_url = save_signature_image(file_storage)
    except Exception:
        return jsonify({"error": "Unable to process signature image"}), 400
    settings = load_settings()
    settings["signature_url"] = signature_url
    save_settings(settings)
    return jsonify({"status": "saved", "signature_url": signature_url})

@app.route("/api/submit", methods=["POST"])
def submit():
    data = request.json
    data["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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


@app.route("/api/batch-summary")
def batch_summary():
    records, institute = get_filtered_records()
    summary = summarize_batch(records)
    summary["institute"] = institute
    return jsonify(summary)


@app.route("/api/submit-batch", methods=["POST"])
def submit_batch():
    payload = request.json or {}
    institute = (payload.get("institute_name") or "").strip()
    if not institute:
        return jsonify({"error": "Institute is required"}), 400

    records = load_records()
    saved_records = [r for r in records if (r.get("institute_name") or "").strip() == institute and not r.get("submitted_at")]
    if not saved_records:
        return jsonify({"error": "No saved ID cards are pending submission for this institute"}), 400

    submitted_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
def get_records():
    records, institute = get_filtered_records()
    return jsonify({"records": records, "institute": institute})

@app.route("/api/delete/<serial_no>", methods=["DELETE"])
def delete_record(serial_no):
    records = load_records()
    records = [r for r in records if r.get("serial_no") != serial_no]
    save_records(records)
    # Delete photo
    photo_path = os.path.join(UPLOAD_DIR, f"{serial_no}.jpg")
    if os.path.exists(photo_path):
        os.remove(photo_path)
    return jsonify({"status": "deleted"})

@app.route("/api/export-excel")
def export_excel():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    records, institute_filter = get_filtered_records()
    wb = Workbook()
    ws = wb.active
    ws.title = "ID Card Records"
    headers = ["Serial No", "Profile Type", "Name", "Course/Designation", "Employee ID", "Department",
               "Date of Birth", "Contact No", "Blood Group", "Address", "Valid Upto",
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
        ws.cell(row=row, column=4, value=rec.get("course") or rec.get("designation", ""))
        ws.cell(row=row, column=5, value=rec.get("employee_id", ""))
        ws.cell(row=row, column=6, value=rec.get("department", ""))
        ws.cell(row=row, column=7, value=rec.get("dob", ""))
        ws.cell(row=row, column=8, value=rec.get("contact", ""))
        ws.cell(row=row, column=9, value=rec.get("blood_group", ""))
        ws.cell(row=row, column=10, value=rec.get("address", ""))
        ws.cell(row=row, column=11, value=rec.get("valid_upto", ""))
        ws.cell(row=row, column=12, value=rec.get("institute_name", ""))
        ws.cell(row=row, column=13, value=f"{rec.get('serial_no', '')}.jpg")
        ws.cell(row=row, column=14, value=rec.get("saved_at", ""))
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
def export_zip():
    records, institute_filter = get_filtered_records()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for rec in records:
            serial = rec.get("serial_no", "")
            photo_path = os.path.join(UPLOAD_DIR, f"{serial}.jpg")
            if os.path.exists(photo_path):
                zf.write(photo_path, f"photos/{serial}.jpg")
        # Also add JSON export
        zf.writestr("records.json", json.dumps(records, indent=2))
    buf.seek(0)
    institute = institute_filter or (records[0].get("institute_name", "IDCardRecords") if records else "IDCardRecords")
    institute_safe = "".join(c for c in institute if c.isalnum() or c in "_ -")
    return send_file(buf, as_attachment=True,
                     download_name=f"{institute_safe}_IDCards_Export_{datetime.now().strftime('%Y%m%d')}.zip",
                     mimetype="application/zip")

if __name__ == "__main__":
    app.run(debug=True, port=5050)
