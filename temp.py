from paddleocr import PaddleOCR

ocr = PaddleOCR(use_textline_orientation=True, lang='en')  # new param, avoids warning
result = ocr.ocr('./samples/2a3mmg_5972.jpg')

# Each page result is a dict
for page in result:
    texts = page.get('rec_texts', [])
    scores = page.get('rec_scores', [])
    for text, score in zip(texts, scores):
        print(f"Detected: {text} (confidence={score:.2f})")
