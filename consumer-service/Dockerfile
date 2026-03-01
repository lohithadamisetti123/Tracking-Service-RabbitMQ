FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=300 --progress-bar=off -r requirements.txt

COPY src/ ./src

WORKDIR /app/src

ENV CONSUMER_PORT=8001

CMD ["uvicorn", "consumer:app", "--host", "0.0.0.0", "--port", "8001"]
