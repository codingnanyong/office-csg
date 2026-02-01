FROM python:3.12-slim
WORKDIR /app
COPY measurement/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY measurement ./measurement
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "measurement.main:app", "--host", "0.0.0.0", "--port", "8000"]
