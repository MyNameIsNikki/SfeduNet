FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/logs /app/ml_model

EXPOSE 8000 5000

CMD ["sh", "-c", "mlflow server --host 0.0.0.0 --port 5000 & python etl_project.py api"]