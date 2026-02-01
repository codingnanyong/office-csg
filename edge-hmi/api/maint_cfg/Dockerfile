FROM python:3.12-slim
WORKDIR /app
COPY maint_cfg/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY maint_cfg ./maint_cfg
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "maint_cfg.main:app", "--host", "0.0.0.0", "--port", "8000"]
