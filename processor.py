import os
import json
import subprocess
import requests
import pandas as pd
from pdf2image import convert_from_path
import gdown

# --- CONFIGURATION & ENDPOINTS ---
BASE_URL = "https://devbackend.readwriteds.com"
LOGIN_URL = f"{BASE_URL}/api/v1/auth/login"
UPLOAD_URL = f"{BASE_URL}/api/v1/uploads"
PARSE_URL = f"{BASE_URL}/api/v1/admin/courses/bulk-upload/parse"
SUBMIT_URL = f"{BASE_URL}/api/v1/admin/courses/bulk-upload/submit"

def get_auth_token(email, password):
    print(f"\n[STAGE 1] 🔐 Authenticating user: {email}...")
    res = requests.post(LOGIN_URL, json={"email": email, "password": password})
    print(f"  -> API Response [{res.status_code}]: {res.text}")
    res.raise_for_status()
    return res.json().get('token', res.json().get('accessToken'))

def upload_image(filepath, token):
    print(f"  -> ☁️ Uploading {os.path.basename(filepath)}...")
    with open(filepath, 'rb') as f:
        res = requests.post(
            f"{UPLOAD_URL}?prefix=course_slides", 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (os.path.basename(filepath), f, 'image/jpeg')}
        )
    print(f"     -> API Response [{res.status_code}]: {res.text}")
    res.raise_for_status()
    return res.json()['url']

def extract_images_from_pptx(file_path):
    print(f"\n[STAGE 3] 🎬 Commencing media extraction for PPTX...")
    images_dir = "extracted_slides"
    os.makedirs(images_dir, exist_ok=True)
    
    print("  -> ⚙️ Converting PPTX to PDF via LibreOffice...")
    subprocess.run(['libreoffice', '--headless', '--convert-to', 'pdf', file_path, '--outdir', '.'], check=True)
    target_pdf = file_path.replace('.pptx', '.pdf')
    
    print("  -> ✂️ Slicing PDF into individual images...")
    slides = convert_from_path(target_pdf, dpi=200)
    image_paths = []
    
    for i, slide in enumerate(slides):
        img_path = f"{images_dir}/slide_{i+1}.jpg"
        slide.save(img_path, 'JPEG')
        image_paths.append(img_path)
            
    print(f"  -> ✅ Extracted {len(image_paths)} images.")
    return image_paths

def main():
    print("==================================================")
    print("🚀 STARTING LMS BULK UPLOAD PROCESSOR")
    print("==================================================")
    
    raw_payload = os.getenv("CLIENT_PAYLOAD", "{}")
    payload = json.loads(raw_payload)
    course_schema = payload.get("course_schema_json", [])
    file_id = payload.get("file_id")
    file_type = payload.get("file_type", "pdf").lower()
    
    print(f"ℹ️ Loaded Schema Array with {len(course_schema)} lessons.")
    
    token = get_auth_token(os.getenv("LMS_EMAIL"), os.getenv("LMS_PASSWORD"))

    # --- DOWNLOAD FILE ---
    print(f"\n[STAGE 2] 📥 Downloading source file (ID: {file_id})...")
    file_name = f"source_file.{file_type}"
    gdown.download(f"https://drive.google.com/uc?id={file_id}", file_name, quiet=False)

    # --- SMART ROUTING ---
    if file_type == 'pptx':
        extracted_images = extract_images_from_pptx(file_name)
        
        print("\n[STAGE 4] 🚀 Uploading Images to Endpoint...")
        uploaded_urls = [upload_image(img, token) for img in extracted_images]

        print("\n[STAGE 5] 💉 Injecting URLs into Course Schema...")
        for i, lesson in enumerate(course_schema):
            if i < len(uploaded_urls):
                lesson["lesson_slides"] = uploaded_urls[i]
            else:
                lesson["lesson_slides"] = ""
                
            if i == 0 and lesson.get("course_image_url") == "PENDING_COVER_IMAGE":
                lesson["course_image_url"] = uploaded_urls[0]
    else:
        print(f"\n[STAGE 3-5] 📄 Native {file_type.upper()} detected. Bypassing Image Extraction & Scrubbing Placeholders.")
        for lesson in course_schema:
            if lesson.get("lesson_slides") == "PENDING_SLIDES":
                lesson["lesson_slides"] = ""
            if lesson.get("course_image_url") == "PENDING_COVER_IMAGE":
                lesson["course_image_url"] = ""

    # --- GENERATE CSV ---
    csv_filename = "bulk_upload.csv"
    print(f"\n[STAGE 6] 📝 Building CSV file...")
    df = pd.DataFrame(course_schema)
    df.to_csv(csv_filename, index=False)
    print(f"  -> Generated {csv_filename} ({len(df)} rows)")

    # --- PARSE ENDPOINT ---
    print(f"\n[STAGE 7] 📡 Sending CSV to {PARSE_URL}...")
    with open(csv_filename, 'rb') as f:
        parse_res = requests.post(
            PARSE_URL, 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (csv_filename, f, 'text/csv')}
        )
    print(f"  -> API Response [{parse_res.status_code}]: {parse_res.text}")
    parse_res.raise_for_status()
    file_key = parse_res.json()['fileKey']
    
    # --- SUBMIT ENDPOINT ---
    print(f"\n[STAGE 8] 📤 Pushing final payload to {SUBMIT_URL}...")
    submit_res = requests.post(
        SUBMIT_URL, 
        headers={"Authorization": f"Bearer {token}"}, 
        json={"fileKey": file_key, "overwrite": True}
    )
    print(f"  -> API Response [{submit_res.status_code}]: {submit_res.text}")
    submit_res.raise_for_status()
    
    print("==================================================")
    print(f"🎉 PIPELINE COMPLETE! LMS Job ID: {submit_res.json().get('jobId')}")
    print("==================================================")

if __name__ == "__main__":
    main()
