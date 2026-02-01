# data_editor

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-150458?logo=pandas&logoColor=white)](https://pandas.pydata.org/)

Utilities to clean and transform CSV datasets used for IoT/industrial logs.

## 📂 Scripts

- `app/remove_data.py`: Remove invalid rows (non‑digit first column) from all CSVs in `data/`
- `app/replace_data.py`: Normalize `REQUEST_PIC` by replacing `:` with `-`
- `app/transform_data.py`: Split raw logs into structured current/vibration CSVs under `rst/<device>/`

## 🚀 Usage

```bash
cd csg/data_editor/app
python remove_data.py
python replace_data.py
python transform_data.py
```

## 🔐 Notes

- Replace any hard‑coded IPs or device IDs in `transform_data.py` with your environment values before running.
