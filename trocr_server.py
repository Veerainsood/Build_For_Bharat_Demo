from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from transformers import TrOCRProcessor, VisionEncoderDecoderModel
from PIL import Image
import torch, uvicorn, io, os

device = "cpu"

print("Loading TrOCR model...")
processor = TrOCRProcessor.from_pretrained("microsoft/trocr-base-stage1", use_fast=True)
model = VisionEncoderDecoderModel.from_pretrained("microsoft/trocr-base-stage1").to(device)
print("Model loaded on CPU ✅")

app = FastAPI(title="TrOCR CAPTCHA OCR Service")

@app.post("/predict")
async def predict(image: UploadFile = File(None), path: str = Form(None)):
    """
    Either upload an image or pass a file path.
    """
    try:
        if image:
            contents = await image.read()
            image = Image.open(io.BytesIO(contents)).convert("RGB")
        elif path:
            image = Image.open(path).convert("RGB")
        else:
            return JSONResponse({"error": "No image provided"}, status_code=400)

        pixel_values = processor(images=image, return_tensors="pt").pixel_values.to(device)
        with torch.no_grad():
            generated_ids = model.generate(pixel_values, max_length=10)
            text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0].replace(" ", "")
        return {"prediction": text}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5001)
