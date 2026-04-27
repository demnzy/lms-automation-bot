import os
import json
import subprocess
import requests
import pandas as pd
from pdf2image import convert_from_path
import gdown

# --- CONFIGURATION ---
BASE_URL = "https://devbackend.readwriteds.com"
LOGIN_URL = f"{BASE_URL}/api/v1/auth/login"
UPLOAD_URL = f"{BASE_URL}/api/v1/uploads"
PARSE_URL = f"{BASE_URL}/api/v1/admin/courses/bulk-upload/parse"
SUBMIT_URL = f"{BASE_URL}/api/v1/admin/courses/bulk-upload/submit"

def get_auth_token(email, password):
    print(f"\n[STAGE 1] 🔐 Authenticating: {email}...")
    res = requests.post(LOGIN_URL, json={"email": email, "password": password})
    print(f"  -> Response [{res.status_code}]: {res.text[:100]}...")
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
    res.raise_for_status()
    return res.json()['url']

def extract_images_from_pptx(file_path):
    print(f"\n[STAGE 3] 🎬 Slicing PPTX into Slide Images...")
    # Convert PPTX to PDF first using LibreOffice
    subprocess.run(['libreoffice', '--headless', '--convert-to', 'pdf', file_path, '--outdir', '.'], check=True)
    target_pdf = file_path.replace('.pptx', '.pdf')
    
    # Slice PDF into images
    slides = convert_from_path(target_pdf, dpi=200)
    image_paths = []
    for i, slide in enumerate(slides):
        img_path = f"slide_{i+1}.jpg"
        slide.save(img_path, 'JPEG')
        image_paths.append(img_path)
    
    print(f"  -> ✅ Extracted {len(image_paths)} slides from presentation.")
    return image_paths

def main():
    print("==================================================")
    print("🚀 LMS AUTO-INJECTION PROCESSOR STARTING")
    print("==================================================")
    
    # Load Environment Data
    raw_payload = os.getenv("CLIENT_PAYLOAD", "{}")
    payload = json.loads(raw_payload)
    course_schema = payload.get("course_schema_json", [])
    file_id = payload.get("file_id")
    file_type = payload.get("file_type", "pdf").lower()
    
    token = get_auth_token(os.getenv("LMS_EMAIL"), os.getenv("LMS_PASSWORD"))

    # Download from Drive
    print(f"\n[STAGE 2] 📥 Downloading Source File (ID: {file_id})...")
    file_name = f"source_file.{file_type}"
    gdown.download(f"https://drive.google.com/uc?id={file_id}", file_name, quiet=False)

    if file_type == 'pptx':
        # --- PPTX LOGIC: EXTRACT & UPLOAD ---
        extracted_images = extract_images_from_pptx(file_name)
        
        print(f"\n[STAGE 4] 🚀 Uploading {len(extracted_images)} images to LMS storage...")
        uploaded_urls = [upload_image(img, token) for img in extracted_images]

        # Use the first slide as the official Course Cover Image
        global_course_cover = uploaded_urls[0] if uploaded_urls else ""

        print("\n[STAGE 5] 💉 Injecting URLs into ALL rows (Flattening Data)...")
        for i, lesson in enumerate(course_schema):
            # 1. Every row MUST have a valid course cover image URL
            lesson["course_image_url"] = global_course_cover
            
            # 2. Every row gets its specific slide URL (1:1 mapping)
            if i < len(uploaded_urls):
                lesson["lesson_slides"] = uploaded_urls[i]
            else:
                # If AI generated more lessons than we have slides
                lesson["lesson_slides"] = ""
            
            print(f"  -> Hydrated Row {i+1}: {lesson['lesson_title']}")

    else:
        # --- PDF LOGIC: CLEANUP ---
        print(f"\n[STAGE 3-5] 📄 Native PDF: Cleaning up media placeholders...")
        for lesson in course_schema:
            lesson["course_image_url"] = "" # Backends usually allow empty for non-presentation PDFs
            lesson["lesson_slides"] = ""

    # Generate the CSV
    csv_filename = "bulk_upload.csv"
    print(f"\n[STAGE 6] 📝 Compiling CSV: {csv_filename}")
    df = pd.DataFrame(course_schema)
    df.to_csv(csv_filename, index=False)
    print(f"  -> CSV Total Rows: {len(df)}")

    # Step 7: Parse
    print(f"\n[STAGE 7] 📡 Sending CSV to Bulk-Upload Parse...")
    with open(csv_filename, 'rb') as f:
        parse_res = requests.post(
            PARSE_URL, 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (csv_filename, f, 'text/csv')}
        )
    print(f"  -> Parse API Response [{parse_res.status_code}]: {parse_res.text}")
    parse_res.raise_for_status()
    
    file_key = parse_res.json()['fileKey']
    
    # Step 8: Submit
    print(f"\n[STAGE 8] 📤 Submitting Job for Job Execution...")
    submit_res = requests.post(
        SUBMIT_URL, 
        headers={"Authorization": f"Bearer {token}"}, 
        json={"fileKey": file_key, "overwrite": True}
    )
    print(f"  -> Submit API Response [{submit_res.status_code}]: {submit_res.text}")
    submit_res.raise_for_status()
    
    print("\n==================================================")
    print(f"🎉 SUCCESS! LMS Job ID: {submit_res.json().get('jobId')}")
    print("==================================================")

if __name__ == "__main__":
    main()
