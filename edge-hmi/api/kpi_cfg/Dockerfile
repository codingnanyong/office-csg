FROM python:3.12-slim
WORKDIR /app
COPY kpi_cfg/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY kpi_cfg ./kpi_cfg
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "kpi_cfg.main:app", "--host", "0.0.0.0", "--port", "8000"]
