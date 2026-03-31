FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY marketplace_ui/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY marketplace_ui /app/marketplace_ui

EXPOSE 8080

CMD ["python", "-m", "uvicorn", "marketplace_ui.app:app", "--host", "0.0.0.0", "--port", "8080"]
