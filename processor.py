import os
import json
import subprocess
import requests
import pandas as pd
from pdf2image import convert_from_path
import gdown

BASE_URL = "https://devbackend.readwriteds.com"

def get_auth_token(email, password):
    res = requests.post(f"{BASE_URL}/api/v1/auth/login", json={"email": email, "password": password})
    res.raise_for_status()
    return res.json().get('token', res.json().get('accessToken'))

def upload_image(filepath, token):
    with open(filepath, 'rb') as f:
        res = requests.post(
            f"{BASE_URL}/api/v1/uploads?prefix=course_slides", 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (os.path.basename(filepath), f, 'image/jpeg')}
        )
    res.raise_for_status()
    return res.json()['url']

def extract_images_from_file(file_path, file_type):
    images_dir = "extracted_slides"
    os.makedirs(images_dir, exist_ok=True)
    
    target_pdf = file_path
    if file_type == 'pptx':
        # Convert PPTX to PDF silently via LibreOffice
        subprocess.run(['libreoffice', '--headless', '--convert-to', 'pdf', file_path, '--outdir', '.'], check=True)
        target_pdf = file_path.replace('.pptx', '.pdf')
    
    # Convert PDF to Images
    slides = convert_from_path(target_pdf, dpi=200)
    image_paths = []
    for i, slide in enumerate(slides):
        img_path = f"{images_dir}/slide_{i+1}.jpg"
        slide.save(img_path, 'JPEG')
        image_paths.append(img_path)
        
    return image_paths

def main():
    payload = json.loads(os.getenv("CLIENT_PAYLOAD", "{}"))
    course_schema = payload.get("course_schema_json", [])
    file_id = payload.get("file_id")
    file_type = payload.get("file_type", "pdf")
    
    token = get_auth_token(os.getenv("LMS_EMAIL"), os.getenv("LMS_PASSWORD"))

    # 1. Download File from Google Drive
    file_name = f"source_file.{file_type}"
    download_url = f"https://drive.google.com/uc?id={file_id}"
    gdown.download(download_url, file_name, quiet=False)

    # 2. Extract & Upload Media
    extracted_images = extract_images_from_file(file_name, file_type)
    uploaded_urls = [upload_image(img, token) for img in extracted_images]

    # 3. Inject URLs into Schema
    for i, lesson in enumerate(course_schema):
        if i < len(uploaded_urls):
            lesson["lesson_video_url"] = uploaded_urls[i]
        else:
            lesson["lesson_video_url"] = ""

    # 4. Generate the exact CSV matching your template
    csv_filename = "bulk_upload.csv"
    df = pd.DataFrame(course_schema)
    df.to_csv(csv_filename, index=False)

    # 5. Parse & Submit to LMS
    with open(csv_filename, 'rb') as f:
        parse_res = requests.post(
            f"{BASE_URL}/api/v1/admin/courses/bulk-upload/parse", 
            headers={"Authorization": f"Bearer {token}"}, 
            files={'file': (csv_filename, f, 'text/csv')}
        )
    parse_res.raise_for_status()
    file_key = parse_res.json()['fileKey']
    
    submit_res = requests.post(
        f"{BASE_URL}/api/v1/admin/courses/bulk-upload/submit", 
        headers={"Authorization": f"Bearer {token}"}, 
        json={"fileKey": file_key, "overwrite": True}
    )
    submit_res.raise_for_status()
    print(f"Success! Job ID: {submit_res.json().get('jobId')}")

if __name__ == "__main__":
    main()
