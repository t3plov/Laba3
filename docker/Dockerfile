FROM python:3.11-slim
WORKDIR /app
COPY services/requirements.txt /app/services/requirements.txt
RUN pip install --no-cache-dir -r /app/services/requirements.txt
COPY services /app/services
COPY mocks /app/mocks
ENV PYTHONPATH=/app/services:/app
CMD ["python", "-m", "gateway.main"]