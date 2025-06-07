# scripts/ocr_extractor.py

import pytesseract
from PIL import Image
from pdf2image import convert_from_path
import numpy as np
import cv2

def preprocess_image(img: Image.Image) -> np.ndarray:
    """Convert to grayscale + Otsu threshold to sharpen text regions."""
    gray = cv2.cvtColor(np.array(img), cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 0, 255,
                              cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return thresh

def extract_text_from_pdf(pdf_path: str) -> str:
    """Convert PDF→images, preprocess, then OCR all pages."""
    full_text = []
    pages = convert_from_path(pdf_path, dpi=300)
    for page in pages:
        proc = preprocess_image(page)
        txt = pytesseract.image_to_string(proc, lang='eng')
        full_text.append(txt)
    return "\n".join(full_text)

if __name__ == "__main__":
    import sys
    pdf_file = sys.argv[1]
    print(extract_text_from_pdf(pdf_file))
