FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline script
COPY shuttles_pipeline.py .

CMD ["python", "shuttles_pipeline.py"]
