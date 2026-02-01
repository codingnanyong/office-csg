FROM python:3.12-slim
WORKDIR /app
COPY shift_cfg/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY shift_cfg ./shift_cfg
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "shift_cfg.main:app", "--host", "0.0.0.0", "--port", "8000"]
