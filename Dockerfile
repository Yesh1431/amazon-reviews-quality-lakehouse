FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-m", "src.cli", "--input-path", "data/raw", "--bronze-path", "data/bronze", "--silver-path", "data/silver", "--gold-path", "data/gold", "--quarantine-path", "data/quarantine"]
