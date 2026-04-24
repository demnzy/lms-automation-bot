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
    print(f"\n[STAGE 1/8] 🔐 Authenticating user: {email}...")
    res = requests.post(LOGIN_URL, json={"email": email, "password": password})
    res.raise_for_status()
    print("  -> ✅ Authentication successful. Token acquired.")
    return res.json().get('token', res.json().get('accessToken'))

def upload_image(filepath, token):
    print(f"  -> ☁️ Uploading {os.path.basename(filepath)} to {UPLOAD_URL}...")
    with open(filepath, 'rb') as f:
        res = requests.post(
            f"{UPLOAD_URL}?prefix=course_slides", 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (os.path.basename(filepath), f, 'image/jpeg')}
        )
    res.raise_for_status()
    url = res.json()['url']
    print(f"     ✅ Success! URL: {url}")
    return url

def extract_images_from_pptx(file_path):
    print(f"\n[STAGE 3/8] 🎬 Commencing media extraction for PPTX: {file_path}")
    images_dir = "extracted_slides"
    os.makedirs(images_dir, exist_ok=True)
    
    print("  -> ⚙️ Converting PPTX to PDF via LibreOffice...")
    subprocess.run(['libreoffice', '--headless', '--convert-to', 'pdf', file_path, '--outdir', '.'], check=True)
    target_pdf = file_path.replace('.pptx', '.pdf')
    print("  -> ✅ PPTX to PDF conversion complete.")
    
    print("  -> ✂️ Slicing PDF into individual high-res images...")
    slides = convert_from_path(target_pdf, dpi=200)
    image_paths = []
    
    for i, slide in enumerate(slides):
        img_path = f"{images_dir}/slide_{i+1}.jpg"
        slide.save(img_path, 'JPEG')
        image_paths.append(img_path)
        if (i + 1) % 5 == 0 or (i + 1) == len(slides):
            print(f"     📸 Saved {i+1} of {len(slides)} slides...")
            
    print(f"  -> ✅ Successfully extracted {len(image_paths)} images!")
    return image_paths

def main():
    print("==================================================")
    print("🚀 STARTING LMS BULK UPLOAD PROCESSOR")
    print("==================================================")
    
    payload = json.loads(os.getenv("CLIENT_PAYLOAD", "{}"))
    course_schema = payload.get("course_schema_json", [])
    file_id = payload.get("file_id")
    file_type = payload.get("file_type", "pdf").lower()
    
    token = get_auth_token(os.getenv("LMS_EMAIL"), os.getenv("LMS_PASSWORD"))

    # --- DOWNLOAD FILE ---
    print(f"\n[STAGE 2/8] 📥 Downloading source file from Drive (ID: {file_id})...")
    file_name = f"source_file.{file_type}"
    download_url = f"https://drive.google.com/uc?id={file_id}"
    gdown.download(download_url, file_name, quiet=False)
    print("  -> ✅ Download complete.")

    # --- SMART ROUTING ---
    if file_type == 'pptx':
        # PATH A: PRESENTATIONS
        extracted_images = extract_images_from_pptx(file_name)
        
        print("\n[STAGE 4/8] 🚀 Uploading Images to Endpoint...")
        uploaded_urls = []
        for img in extracted_images:
            url = upload_image(img, token)
            uploaded_urls.append(url)

        print("\n[STAGE 5/8] 💉 Injecting URLs into Course Schema...")
        for i, lesson in enumerate(course_schema):
            # Inject slide URLs
            if i < len(uploaded_urls):
                lesson["lesson_slides"] = uploaded_urls[i]
                print(f"  -> Injected slide URL into lesson: {lesson.get('lesson_title', f'Lesson {i+1}')}")
            else:
                lesson["lesson_slides"] = ""
                
            # Inject course cover image (using slide 1)
            if i == 0 and lesson.get("course_image_url") == "PENDING_COVER_IMAGE":
                lesson["course_image_url"] = uploaded_urls[0]
                print("  -> Injected Slide 1 as Course Cover Image.")
    else:
        # PATH B: NATIVE PDFs
        print(f"\n[STAGE 3/8] 📄 Native {file_type.upper()} detected. Bypassing Image Extraction.")
        print("\n[STAGE 4/8] ⏩ Bypassing Image Uploads.")
        
        print("\n[STAGE 5/8] 🧹 Scrubbing placeholders from AI Schema...")
        for lesson in course_schema:
            if lesson.get("lesson_slides") == "PENDING_SLIDES":
                lesson["lesson_slides"] = ""
            if lesson.get("course_image_url") == "PENDING_COVER_IMAGE":
                lesson["course_image_url"] = ""
        print("  -> ✅ Placeholders removed.")

    # --- GENERATE CSV ---
    csv_filename = "bulk_upload.csv"
    print(f"\n[STAGE 6/8] 📝 Building CSV file from JSON Schema...")
    df = pd.DataFrame(course_schema)
    df.to_csv(csv_filename, index=False)
    print(f"  -> ✅ CSV generated successfully: {csv_filename} ({len(df)} rows)")

    # --- PARSE ENDPOINT ---
    print(f"\n[STAGE 7/8] 📡 Sending CSV to {PARSE_URL}...")
    with open(csv_filename, 'rb') as f:
        parse_res = requests.post(
            PARSE_URL, 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (csv_filename, f, 'text/csv')}
        )
    parse_res.raise_for_status()
    file_key = parse_res.json()['fileKey']
    print(f"  -> ✅ Parse successful! Received fileKey: {file_key}")
    
    # --- SUBMIT ENDPOINT ---
    print(f"\n[STAGE 8/8] 📤 Pushing final payload to {SUBMIT_URL}...")
    submit_payload = {
        "fileKey": file_key,
        "overwrite": True
    }
    submit_res = requests.post(
        SUBMIT_URL, 
        headers={"Authorization": f"Bearer {token}"}, 
        json=submit_payload
    )
    submit_res.raise_for_status()
    job_id = submit_res.json().get('jobId')
    
    print("==================================================")
    print(f"🎉 PIPELINE COMPLETE! LMS Job ID: {job_id}")
    print("==================================================")

if __name__ == "__main__":
    main()
