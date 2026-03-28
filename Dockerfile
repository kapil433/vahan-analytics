# API only (scraper UI + dashboard + data endpoints). Browsers inside container need extra setup for Selenium.
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements-render.txt ./
RUN pip install --no-cache-dir -r requirements-render.txt

COPY . .

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
