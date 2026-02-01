FROM python:3.12-slim
WORKDIR /app
COPY worker_mst/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY worker_mst ./worker_mst
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "worker_mst.main:app", "--host", "0.0.0.0", "--port", "8000"]
