FROM python:3.12-slim
WORKDIR /app
COPY equip_mst/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY equip_mst ./equip_mst
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "equip_mst.main:app", "--host", "0.0.0.0", "--port", "8000"]
