FROM python:3.12-slim
WORKDIR /app
COPY hmi_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY hmi_api ./hmi_api
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "hmi_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
