FROM python:3.12-slim
WORKDIR /app
COPY alarm_cfg/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY alarm_cfg ./alarm_cfg
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "alarm_cfg.main:app", "--host", "0.0.0.0", "--port", "8000"]
