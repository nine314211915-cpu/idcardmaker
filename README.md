# Medical College ID Card Data Collection System

## Setup

```bash
pip install -r requirements.txt
python app.py
```

Server runs at: http://localhost:5050

## Pages

- **http://localhost:5050/** — Data Entry Page (Student / Lecturer)
- **http://localhost:5050/admin** — Admin Panel (View / Download)

## Features

### Entry Page
- Select Student or Lecturer profile (persists across entries)
- Upload photo → AI auto-crops to passport style (3:4 ratio, 300×400px)
- Auto-generates Serial No: `ID-YYYYMMDD-XXXX` on photo upload
- Live ID card preview updates as you type
- All fields + inserted date (editable)
- One-click submit → saves to `records.json`

### Admin Page
- View all records in a table
- Filter by type, search by name/serial
- View full ID card preview in modal
- Download individual photo as JPG (filename = serial number)
- Export all data as Excel (.xlsx)
- Download ZIP with all photos + JSON data
- Delete records

## Data Storage

- Records: `records.json`
- Photos: `static/uploads/{SERIAL_NO}.jpg`

## File Structure

```
idcard/
├── app.py              — Flask backend
├── requirements.txt
├── records.json        — Auto-created on first submission
├── static/
│   └── uploads/        — Cropped passport photos (named by serial no)
└── templates/
    ├── index.html      — Data entry page
    └── admin.html      — Admin panel
```
