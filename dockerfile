# ใช้ image python
FROM python:3.14
 
WORKDIR /app
 
COPY . .

# คัดลอก credential ลง container
COPY itsm-pipeline-bac89c675c5e.json /app/itsm-pipeline-bac89c675c5e.json

RUN pip install --no-cache-dir -r requirements.txt
 
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]

