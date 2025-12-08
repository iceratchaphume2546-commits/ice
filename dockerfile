# ใช้ image python เบา ๆ
FROM python:3.11-slim
 
# กำหนด working directory
WORKDIR /app
 
# คัดลอกทุกไฟล์จาก repo เข้า container
COPY . .
 
# ติดตั้ง dependencies
RUN pip install --no-cache-dir -r requirements.txt || true
 
# ตั้ง environment variable (Cloud Run จะ override ได้ภายหลัง)
ENV PYTHONUNBUFFERED=1
 
# รัน script หลักของ pipeline
CMD ["python", "sync_erp_data.py"]