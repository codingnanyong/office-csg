FROM python:3.12-slim
WORKDIR /app
COPY status_his/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY status_his ./status_his
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "status_his.main:app", "--host", "0.0.0.0", "--port", "8000"]
