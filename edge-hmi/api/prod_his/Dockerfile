FROM python:3.12-slim
WORKDIR /app
COPY prod_his/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY prod_his ./prod_his
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "prod_his.main:app", "--host", "0.0.0.0", "--port", "8000"]
