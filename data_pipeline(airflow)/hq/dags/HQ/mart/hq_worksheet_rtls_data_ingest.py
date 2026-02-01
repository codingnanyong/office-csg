import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.hooks.postgres_hook import PostgresHelper

# ────────────────────────────────────────────────────────────────────────────
# 1️⃣ 설정 (Config & Logging)
# ────────────────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)

CONN_ID_v1 = "pg_fdw_v1_hq"
CONN_ID_v2 = "pg_fdw_v2_hq"
postgres_helper_v1 = PostgresHelper(CONN_ID_v1)
postgres_helper_v2 = PostgresHelper(CONN_ID_v2)

SCHEMA_NAME = "services"
SQL_WS = "SELECT ws_no FROM services.ws"

TABLES = {
    "tags" :
    """
       SELECT DISTINCT at.tagid,RIGHT(at.owner_tag_name, 20) AS ws_no ,tp.tag_type as tag_type
        FROM rtls.at_tag_xy at
        JOIN rtls.tb_pass_type_mapper tp 
            ON SUBSTRING(at.owner_tag_name FROM LENGTH(at.owner_tag_name) - 22 FOR 2) = tp.pass_type
        WHERE (tagid,event_dt) in (SELECT tagid ,MAX(event_dt) FROM rtls.at_tag_xy WHERE tagid IS NOT NULL GROUP BY tagid)
        and RIGHT(at.owner_tag_name, 20) is not null and RIGHT(at.owner_tag_name, 20) like 'WS%'; 
    """,
    "tag_status" :
    """
        SELECT DISTINCT RIGHT(at.owner_tag_name, 20) AS wsno ,at.tagid AS tag_id,
            CASE WHEN loc.level2 = 'UPP' THEN 'PRT' ELSE loc.level2 END AS op_cd,
            CASE WHEN RIGHT(loc.loc_name, 2) = '입고' THEN 'I' 
                    WHEN RIGHT(loc.loc_name, 2) = '투입' THEN 'T' 
                    WHEN RIGHT(loc.loc_name, 2) = '완료' THEN 'O' 
                    WHEN RIGHT(loc.loc_name, 2) = '보관' THEN 'K'
                    WHEN RIGHT(loc.loc_name, 2) = '중단' THEN 'S'
                    WHEN RIGHT(loc.loc_name, 2) = '대기' THEN 'H'
                    WHEN RIGHT(loc.loc_name, 2) = '도착' THEN 'A'
                    WHEN RIGHT(loc.loc_name, 2) = '발행' THEN 'P'
                    WHEN RIGHT(loc.loc_name, 2) = '삭제' THEN 'D'
            ELSE NULL END AS status,
            case when at.zone_id ='N/A' then null else at.zone_id end as zone,
            at.coordi_sys as floor,
            SPLIT_PART(position_xy, '/', 1) AS x,
            SPLIT_PART(position_xy, '/', 2) AS y
        FROM rtls.at_tag_xy at
        LEFT JOIN rtls.tb_process_loc loc ON loc.process_loc = at.zone_id
        WHERE (tagid,event_dt) in (SELECT tagid ,MAX(event_dt) FROM rtls.at_tag_xy WHERE tagid IS NOT NULL GROUP BY tagid)
                and RIGHT(at.owner_tag_name, 20) is not null and RIGHT(at.owner_tag_name, 20) like 'WS%'; 
    """,
    "tag_his":
    """
        SELECT DISTINCT RIGHT(ws.owner_tag_name, 20) AS ws_no, ws.tagid AS tag_id,
            loc.level2 AS opcd,
            CASE
                WHEN RIGHT(loc.loc_name, 2) = '입고' THEN 'I'
                WHEN RIGHT(loc.loc_name, 2) = '투입' THEN 'T'
                WHEN RIGHT(loc.loc_name, 2) = '완료' THEN 'O'
                WHEN RIGHT(loc.loc_name, 2) = '보관' THEN 'K'
                WHEN RIGHT(loc.loc_name, 2) = '중단' THEN 'S'
                WHEN RIGHT(loc.loc_name, 2) = '대기' THEN 'H'
                WHEN RIGHT(loc.loc_name, 2) = '도착' THEN 'A'
                WHEN RIGHT(loc.loc_name, 2) = '발행' THEN 'P'
                WHEN RIGHT(loc.loc_name, 2) = '삭제' THEN 'D'
                ELSE NULL END AS status,
            ws.zone_id AS zone, ws.coordi_sys AS floor,
            MIN(ws.event_dt) AS now,
            COALESCE(LEAD(MIN(ws.event_dt)) OVER (PARTITION BY ws.tagid ORDER BY MIN(ws.event_dt) ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING), NOW()) AS next,
            COALESCE(LEAD(MIN(ws.event_dt)) OVER (PARTITION BY ws.tagid ORDER BY MIN(ws.event_dt) ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING), NOW()) - MIN(ws.event_dt) AS diff
        FROM rtls.at_tag_xy AS ws
        LEFT JOIN rtls.tb_process_loc loc ON ws.zone_id = loc.process_loc
        WHERE RIGHT(ws.owner_tag_name, 20) LIKE 'WS%'
            AND ws.zone_id <> 'N/A'
        GROUP BY RIGHT(ws.owner_tag_name, 20), ws.tagid,loc.level2,loc.loc_name,ws.zone_id,ws.coordi_sys
        ORDER BY ws_no,tag_id,now asc;
    """
}

# ────────────────────────────────────────────────────────────────────────────
# 2️⃣ DAG 정의 (DAG Definition & Task Dependencies)
# ────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hq_worksheet_rtls_data_ingest",
    default_args=default_args,
    description="HQ RTLS Coordinate Data Ingest",
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["HQ","WorkSheet", "RTLS"]
) as dag:
    
    def format_datetime(value):
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, timedelta):
            return str(value)  
        return value

    task_fetch_ws = PythonOperator(
        task_id="fetch_ws",
        python_callable=postgres_helper_v2.execute_query,
        op_args=[SQL_WS, "fetch_ws", "ws_data"],
    )

    parallel_tasks = []
    for table_name, sql_query in TABLES.items():

        check_task = PythonOperator(
            task_id=f"check_{table_name}",
            python_callable=postgres_helper_v2.check_table,
            op_args=[SCHEMA_NAME, table_name],
        )

        clean_task = PythonOperator(
            task_id=f"clean_{table_name}",
            python_callable=postgres_helper_v2.clean_table,
            op_args=[SCHEMA_NAME, table_name],
            trigger_rule="all_success",
        )
        
        if table_name == 'tag_his' :
            fetch_task = PythonOperator(
                task_id=f"fetch_{table_name}",
                python_callable=lambda ti, tn=table_name: ti.xcom_push(
                    key=f"{tn}_data",
                    value=[
                        tuple(
                            format_datetime(col) if isinstance(col, (datetime, timedelta)) else col 
                            for col in row
                        )
                        for row in (postgres_helper_v1.execute_query(
                            sql_query, task_id=f"fetch_{tn}", xcom_key=f"{tn}_data") or [])
                    ]
                ),
                trigger_rule="all_success",
            )
        else :
            fetch_task = PythonOperator(
                task_id=f"fetch_{table_name}",
                python_callable=postgres_helper_v1.execute_query,
                op_args=[sql_query, f"fetch_{table_name}", f"{table_name}_data"],
                trigger_rule="all_success",
            )

        insert_task = PythonOperator(
            task_id=f"insert_{table_name}",
            python_callable=lambda ti, tn=table_name: postgres_helper_v2.insert_data(
                SCHEMA_NAME, tn,
                [tuple(row) for row in (ti.xcom_pull(task_ids=f"fetch_{tn}", key=f"{tn}_data") or [])]
                ),
                trigger_rule="all_success",
        )

        task_fetch_ws >> check_task >> clean_task >> fetch_task >> insert_task
        parallel_tasks.append(insert_task)