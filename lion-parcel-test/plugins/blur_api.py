import cv2
import numpy as np
import requests
from fastapi import FastAPI
from openai import OpenAI
import uvicorn

app = FastAPI()

client = OpenAI() 

def is_image_blurry(image_bytes, threshold=100.0):
    # Mengonversi bytes gambar ke format OpenCV
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return True, 0
    # Konversi ke Grayscale untuk perhitungan
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # Menghitung Variance of Laplacian (Skor Ketajaman)
    variance = cv2.Laplacian(gray, cv2.CV_64F).var()
    return variance < threshold, variance

@app.get("/detect-image")
def detect_image(url: str):
    try:
        # 1. Ambil gambar dari URL
        response = requests.get(url, timeout=10)
        img_bytes = response.content
        
        # 2. Cek apakah gambar blur
        blurry, score = is_image_blurry(img_bytes)
        
        if blurry:
            return {
                "status": "success",
                "is_blur": True,
                "blur_score": score,
                "description": "Image is too blurry to be described."
            }
        
        # 3. Jika tajam, deskripsikan pakai OpenAI Vision
        try:
            ai_response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe this image content concisely."},
                            {"type": "image_url", "image_url": {"url": url}}
                        ],
                    }
                ],
            )
            description = ai_response.choices[0].message.content
        except Exception as e:
            # Jika OpenAI Error (Quota Habis), berikan deskripsi fallback
            print(f"OpenAI Error: {e}")
            description = "Gambar terdeteksi tajam, namun deskripsi tidak tersedia (OpenAI Quota Limit)."

        return {
            "status": "success",
            "is_blur": False,
            "blur_score": score,
            "description": description
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)