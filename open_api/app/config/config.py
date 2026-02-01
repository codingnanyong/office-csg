import json
import os

class DBConfig:
    def __init__(self, config_path="app/config/db.json"):
        with open(config_path, "r") as config_file:
            config_data = json.load(config_file)

        self.DATABASE_URLS = {
            "hq": config_data["database"].get("FDW_HQ_URL"),
            "vj": config_data["database"].get("FDW_VJ_URL")
        }

    def get_database_url(self, db_name="hq"):
        return self.DATABASE_URLS.get(db_name, None)

db_config = DBConfig()
