import sys
import logging
import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from plugins.hooks.postgres_hook import PostgresHelper

# ─────────────────────────────────────────────
# #️⃣ 기본 설정
# ─────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger(__name__)
CONN_ID_V1 = "pg_fdw_v1_vj"
CONN_ID_V2 = "pg_fdw_v2_vj"
pg_v1 = PostgresHelper(CONN_ID_V1)
pg_v2 = PostgresHelper(CONN_ID_V2)

SCHEMA_NAME = 'services'
SQL_IP = "SELECT zone, mach_id,ymd,total_qty FROM services.ip_performacne"
SQL_STOCK = '''
    SELECT part_cd, stock as stock_qty
    FROM (
        SELECT part_cd, stock,
            ROW_NUMBER() OVER (PARTITION BY part_cd ORDER BY stock_date DESC) AS rn
        FROM CMMS.PRT_STOCK_DAY
        WHERE WH_CD = '1B2IA'
    ) sub
    WHERE rn = 1;
'''
XCOM_COLUMNS = {
    "workorder_intervals": ["zone", "mach_id", "part_cd", "current_wo_date", "previous_wo_date", "days_between_wo_dates"],
    "cycle_dt": ["zone", "mach_id", "part_cd", "cycle_dt"],
}
# ─────────────────────────────────────────────
# ⏱ 날짜 범위 계산 함수
# ─────────────────────────────────────────────
def get_date_range(p_date: str):
    if len(p_date) == 4:
        v_start_date = datetime.strptime(f"{p_date}1231", "%Y%m%d")
        v_end_date = datetime.strptime(f"{int(p_date) - 1}0101", "%Y%m%d")
    elif len(p_date) == 6:
        y, m = int(p_date[:4]), int(p_date[4:])
        start_dt = datetime(y, m, 1) + relativedelta(months=1) - timedelta(days=1)
        end_dt = start_dt - relativedelta(months=2)
        v_start_date = start_dt
        v_end_date = end_dt
    else:
        raise ValueError("Invalid p_date format")
    return v_start_date, v_end_date
# ─────────────────────────────────────────────
# 1️⃣ Spare Part Top 10
# ─────────────────────────────────────────────
def spare_part_top10(group_by_type, p_date, **kwargs):
    v_start_date, v_end_date = get_date_range(p_date)
    machine_col = "ip.cmms_machine_id" if group_by_type == "zone_machine" else "NULL AS cmms_machine_id"
    group_by_machine = "ip.cmms_machine_id," if group_by_type == "zone_machine" else ""
    select_machine = "cmms_machine_id AS machine," if group_by_type == "zone_machine" else "NULL AS machine,"
    partition_by_machine = ", cmms_machine_id" if group_by_type == "zone_machine" else ""

    sql = f"""
    WITH ip_list AS (
        SELECT zone, machine_name, mes_machine_id, cmms_machine_id
        FROM services.mes_cmms_mapping
        WHERE use <> 'N'
    ),
    wof_part AS (
        SELECT o.company_cd, o.wo_yymm, o.wo_orgn, o.wo_no,
               o.wo_date, o.mach_id, p.part_cd, COALESCE(p.outgoing_qty, 0) AS outgoing_qty
        FROM cmms.wof_order o
        JOIN cmms.wof_order_part p 
            ON o.wo_yymm = p.wo_yymm AND o.wo_orgn = p.wo_orgn AND o.wo_no = p.wo_no
        WHERE o.company_cd = '1'
          AND o.wo_date BETWEEN '{v_end_date:%Y-%m-%d}' AND '{v_start_date:%Y-%m-%d}'
    ),
    part_mst AS (
        SELECT part_cd, part_nm_en FROM cmms.bas_part WHERE company_cd = '1'
    ),
    part_outgoing_data AS (
        SELECT ip.zone, {machine_col}, pt.part_cd, mt.part_nm_en,
               SUM(pt.outgoing_qty) AS total_outgoing_qty, COUNT(*) AS wo_cnt
        FROM wof_part pt
        JOIN ip_list ip ON pt.mach_id = ip.cmms_machine_id
        JOIN part_mst mt ON pt.part_cd = mt.part_cd
        GROUP BY ip.zone, {group_by_machine} pt.part_cd, mt.part_nm_en
    ),
    ranked_parts AS (
        SELECT zone, {select_machine} part_cd, part_nm_en, wo_cnt, total_outgoing_qty,
               ROW_NUMBER() OVER (PARTITION BY zone{partition_by_machine} ORDER BY total_outgoing_qty DESC) AS rn
        FROM part_outgoing_data
    )
    SELECT zone, machine, part_cd, part_nm_en, wo_cnt, total_outgoing_qty, rn
    FROM ranked_parts
    WHERE rn <= 10
    ORDER BY zone, machine, rn;
    """
    pg_v1.execute_query(sql=sql, task_id=f"spare_part_top10_{group_by_type}_{p_date}", xcom_key="spare_top10_data", **kwargs)
# ─────────────────────────────────────────────
# 2️⃣ WorkOrder Intervals
# ─────────────────────────────────────────────
def workorder_intervals(group_by_type, p_date, **kwargs):
    v_start_date, v_end_date = get_date_range(p_date)
    group_by_cols = "ip.zone, o.mach_id, p.part_cd" if group_by_type == "zone_machine" else "ip.zone, p.part_cd"
    select_mach_col = "ow.mach_id," if group_by_type == "zone_machine" else "NULL AS mach_id,"

    sql = f"""
        WITH ip_list AS (
            SELECT zone, machine_name, mes_machine_id, cmms_machine_id FROM services.mes_cmms_mapping WHERE use <> 'N'
        ),
        ordered_wo AS (
            SELECT o.mach_id, p.part_cd, o.wo_date,
                LAG(o.wo_date) OVER (PARTITION BY {group_by_cols} ORDER BY o.wo_date) AS previous_wo_date
            FROM cmms.wof_order o
            JOIN cmms.wof_order_part p ON o.wo_yymm = p.wo_yymm AND o.wo_orgn = p.wo_orgn AND o.wo_no = p.wo_no
            JOIN ip_list ip ON o.mach_id = ip.cmms_machine_id
            WHERE o.company_cd = '1' AND o.wo_date BETWEEN '{v_end_date:%Y-%m-%d}' AND '{v_start_date:%Y-%m-%d}'
        )
        SELECT ip.zone, {select_mach_col} ow.part_cd, ow.wo_date AS current_wo_date, ow.previous_wo_date,
            CASE WHEN ow.previous_wo_date IS NOT NULL THEN EXTRACT(DAY FROM ow.wo_date - ow.previous_wo_date)
                    ELSE NULL END AS days_between_wo_dates
        FROM ordered_wo ow
        JOIN services.mes_cmms_mapping ip ON ow.mach_id = ip.cmms_machine_id
        ORDER BY ip.zone, {'ow.mach_id,' if group_by_type == 'zone_machine' else ''} ow.part_cd, current_wo_date;
    """
    pg_v1.execute_query(sql=sql, task_id=f"workorder_intervals_{group_by_type}_{p_date}",
                        xcom_key="workorder_intervals_data", **kwargs)
# ─────────────────────────────────────────────
# 3️⃣ Calculate Cycle Date
# ─────────────────────────────────────────────
def calculate_cycle_dt(group_by_type, p_date, **kwargs):
    ti = kwargs["ti"]
    v_start_date, v_end_date = get_date_range(p_date)

    data = ti.xcom_pull(key="workorder_intervals_data", task_ids=f"{group_by_type}_{p_date}.workorder_intervals")
    if not data:
        logger.warning(f"[WARNING] No workorder_intervals_data found from XCom for {group_by_type} {p_date}")
        return

    df = pd.DataFrame(data, columns=XCOM_COLUMNS["workorder_intervals"])
    df["current_wo_date"] = pd.to_datetime(df["current_wo_date"])
    df = df[(df["current_wo_date"] >= v_end_date) & (df["current_wo_date"] <= v_start_date)]
    df = df[df["previous_wo_date"].notnull()]

    group_cols = ["zone", "part_cd"]
    if group_by_type == "zone_machine":
        group_cols.insert(1, "mach_id")

    result_df = (
        df.groupby(group_cols)
        .agg(cycle_dt=('days_between_wo_dates', 'mean'), count=('days_between_wo_dates', 'count'))
        .reset_index()
    )
    result_df = result_df[result_df['count'] > 2]
    result_df["cycle_dt"] = pd.to_numeric(result_df["cycle_dt"], errors="coerce").round(0).astype("Int64")
    result_df.drop(columns=["count"], inplace=True)

    result = result_df.to_records(index=False).tolist()
    ti.xcom_push(key="cycle_dt_data", value=result)
    logger.info(f"[SUCCESS] calculate_cycle_dt completed. Rows: {len(result)}")
# ─────────────────────────────────────────────
# 4️⃣ Calculate MIN/MAX by part
# ─────────────────────────────────────────────
def calculate_minmax(group_by_type, p_date, **kwargs):
    ti = kwargs["ti"]
    v_start_date, v_end_date = get_date_range(p_date)

    cycle_dt_data = ti.xcom_pull(key="cycle_dt_data", task_ids=f"{group_by_type}_{p_date}.calculate_cycle_dt")
    if not cycle_dt_data:
        logger.warning(f"[WARNING] No cycle_dt_data found in XCom")
        return

    if group_by_type == "zone_machine":
        columns = ["zone", "mach_id", "part_cd", "cycle_dt"]
    else:
        columns = ["zone", "part_cd", "cycle_dt"]

    if any(len(row) != len(columns) for row in cycle_dt_data):
        logger.warning(f"[WARNING] XCom cycle_dt_data column count mismatch. Expected={len(columns)}")
        return

    cycle_df = pd.DataFrame(cycle_dt_data, columns=columns)
    if group_by_type != "zone_machine":
        cycle_df["mach_id"] = None

    sql = f"""
        SELECT ip.zone, ip.cmms_machine_id AS mach_id, o.wo_date, p.part_cd, COALESCE(p.outgoing_qty, 0) AS outgoing_qty
        FROM services.mes_cmms_mapping ip
        JOIN cmms.wof_order o ON ip.cmms_machine_id = o.mach_id
        JOIN cmms.wof_order_part p ON o.wo_yymm = p.wo_yymm AND o.wo_orgn = p.wo_orgn AND o.wo_no = p.wo_no
        WHERE ip.use <> 'N'
        AND o.company_cd = '1'
        AND o.wo_date BETWEEN '{v_end_date.strftime("%Y-%m-%d")}' AND '{v_start_date.strftime("%Y-%m-%d")}'
    """
    records = pg_v1.execute_query(
        sql=sql,
        task_id=f"calculate_minmax_{group_by_type}_{p_date}",
        xcom_key="minmax_data",
        **kwargs
    )

    if not records:
        logger.warning(f"[WARNING] No wof_order data found.")
        return

    wo_df = pd.DataFrame(records, columns=["zone", "mach_id", "wo_date", "part_cd", "outgoing_qty"])
    wo_df["wo_date"] = pd.to_datetime(wo_df["wo_date"])

    if group_by_type != "zone_machine":
        wo_df["mach_id"] = None

    merged_df = pd.merge(wo_df, cycle_df, on=["zone", "mach_id", "part_cd"], how="inner")
    merged_df = merged_df[merged_df["cycle_dt"].notnull()]
    merged_df["cycle_dt"] = pd.to_numeric(merged_df["cycle_dt"], errors="coerce").fillna(0)

    today = pd.Timestamp("today").normalize()
    merged_df["days_since"] = (today - merged_df["wo_date"]).dt.days
    merged_df["period_start"] = today - (merged_df["days_since"] // merged_df["cycle_dt"].replace(0, 1)) * merged_df["cycle_dt"].replace(0, 1).astype("timedelta64[D]")
    merged_df["period_end"] = merged_df["period_start"] + merged_df["cycle_dt"].astype("timedelta64[D]")

    group_cols = ["zone", "part_cd"]
    if group_by_type == "zone_machine":
        group_cols.insert(1, "mach_id")

    result_df = (
        merged_df.groupby(group_cols)
        .agg(
            min_qty=("outgoing_qty", lambda x: x[x > 0].min() if any(x > 0) else None),
            max_qty=("outgoing_qty", "max")
        )
        .reset_index()
    )

    result = result_df.to_records(index=False).tolist()
    ti.xcom_push(key="minmax_data", value=result)
    logger.info(f"[SUCCESS] calculate_minmax completed. Rows: {len(result)}")
# ─────────────────────────────────────────────────────────────
# 5️⃣ Calculate IP Performance By WorkOrder
# ─────────────────────────────────────────────────────────────
def calculate_ip_wo_performance(group_by_type, p_date, **kwargs):
    ti = kwargs["ti"]
    logger.info(f"[START] calculate_ip_wo_performance | group_by_type={group_by_type}, p_date={p_date}")

    ip_rst_data = ti.xcom_pull(task_ids="ip_rst", key="ip_rst_data")
    wo_interval_data = ti.xcom_pull(task_ids=f"{group_by_type}_{p_date}.workorder_intervals", key="workorder_intervals_data")

    if not ip_rst_data or not wo_interval_data:
        logger.warning("[WARNING] Missing input data from XCom.")
        return

    wo_columns = ["zone", "mach_id", "part_cd", "current_wo_date", "previous_wo_date", "days_between_wo_dates"]
    wo_df = pd.DataFrame(wo_interval_data, columns=wo_columns)
    if group_by_type != "zone_machine":
        wo_df["mach_id"] = None

    wo_df.drop(columns=["days_between_wo_dates"], inplace=True)
    wo_df["current_wo_date"] = pd.to_datetime(wo_df["current_wo_date"], errors="coerce")
    wo_df["previous_wo_date"] = pd.to_datetime(wo_df["previous_wo_date"], errors="coerce")

    ip_df = pd.DataFrame(ip_rst_data, columns=["zone", "mach_id", "ymd", "total_qty"])
    if group_by_type != "zone_machine":
        ip_df["mach_id"] = None

    ip_df["ymd"] = pd.to_datetime(ip_df["ymd"], format="%Y%m%d", errors="coerce")
    ip_df["total_qty"] = pd.to_numeric(ip_df["total_qty"], errors="coerce").fillna(0)

    join_keys = ["zone", "mach_id"] if group_by_type == "zone_machine" else ["zone"]
    ip_total = pd.merge(wo_df, ip_df, on=join_keys, how="left")

    ip_total["total_qty_filtered"] = 0
    ip_total["total_qty_recent_filtered"] = 0

    valid_mask = ip_total["current_wo_date"].notna() & ip_total["previous_wo_date"].notna()
    ip_total.loc[
        valid_mask & 
        (ip_total["ymd"] >= ip_total["previous_wo_date"]) & 
        (ip_total["ymd"] <= ip_total["current_wo_date"]),
        "total_qty_filtered"
    ] = ip_total["total_qty"]

    ip_total.loc[
        ip_total["current_wo_date"].notna() & (ip_total["ymd"] > ip_total["current_wo_date"]),
        "total_qty_recent_filtered"
    ] = ip_total["total_qty"]

    group_cols = ["zone", "mach_id", "part_cd", "current_wo_date", "previous_wo_date"] if group_by_type == "zone_machine" \
        else ["zone", "part_cd", "current_wo_date", "previous_wo_date"]

    total_agg = ip_total.groupby(group_cols, dropna=False)["total_qty_filtered"].sum().reset_index(name="total_qty")
    recent_agg = ip_total.groupby(group_cols, dropna=False)["total_qty_recent_filtered"].sum().reset_index(name="total_qty_recent")

    result_df = pd.merge(total_agg, recent_agg, on=group_cols, how="outer").fillna(0)

    final_columns = group_cols + ["total_qty", "total_qty_recent"]
    result_df = result_df[final_columns]

    for col in ["current_wo_date", "previous_wo_date"]:
        result_df[col] = pd.to_datetime(result_df[col], errors="coerce")
        result_df[col] = result_df[col].where(pd.notnull(result_df[col]), None)

    result_df = result_df.where(pd.notnull(result_df), None)

    result = [
        tuple(
            val.to_pydatetime() if isinstance(val, pd.Timestamp) and pd.notnull(val)
            else val if pd.notnull(val)
            else None
            for val in row
        )
        for row in result_df.itertuples(index=False, name=None)
    ]

    ti.xcom_push(key="ip_wo_performance_data", value=result)
    logger.info(f"[SUCCESS] calculate_ip_wo_performance completed. Rows: {result}")
# ─────────────────────────────────────────────────────────────
# 6️⃣ Final Analysis Result
# ─────────────────────────────────────────────────────────────
def final_analysis(group_by_type, p_date, **kwargs):
    ti = kwargs["ti"]
    logger.info(f"[START] final_analysis | group_by_type={group_by_type}, p_date={p_date}")

    spare_top10_data = ti.xcom_pull(task_ids=f"{group_by_type}_{p_date}.spare_part_top10", key="spare_top10_data")
    cycle_dt_data = ti.xcom_pull(task_ids=f"{group_by_type}_{p_date}.calculate_cycle_dt", key="cycle_dt_data")
    minmax_data = ti.xcom_pull(task_ids=f"{group_by_type}_{p_date}.calculate_minmax", key="minmax_data")
    ip_wo_perf_data = ti.xcom_pull(task_ids=f"{group_by_type}_{p_date}.calculate_ip_wo_performance", key="ip_wo_performance_data")
    stock_data = ti.xcom_pull(task_ids="latest_stock", key="stock_data")

    if not spare_top10_data or not cycle_dt_data or not minmax_data:
        logger.warning("[WARNING] Missing required input data from XCom.")
        return

    is_zone_machine = group_by_type == "zone_machine"

    spare_columns = ["zone", "mach_id", "part_cd", "part_nm_en", "wo_cnt", "total_outgoing_qty", "rn"]
    spare_df = pd.DataFrame(spare_top10_data, columns=spare_columns)
    if not is_zone_machine:
        spare_df["mach_id"] = None

    cycle_columns = ["zone", "mach_id", "part_cd", "cycle_dt"] if is_zone_machine else ["zone", "part_cd", "cycle_dt"]
    cycle_df = pd.DataFrame(cycle_dt_data, columns=cycle_columns)
    if not is_zone_machine:
        cycle_df["mach_id"] = None

    minmax_columns = ["zone", "mach_id", "part_cd", "min_qty", "max_qty"] if is_zone_machine else ["zone", "part_cd", "min_qty", "max_qty"]
    minmax_df = pd.DataFrame(minmax_data, columns=minmax_columns)
    if not is_zone_machine:
        minmax_df["mach_id"] = None

    ip_perf_columns = ["zone", "mach_id", "part_cd", "current_wo_date", "previous_wo_date", "total_qty", "total_qty_recent"] \
        if is_zone_machine else ["zone", "part_cd", "current_wo_date", "previous_wo_date", "total_qty", "total_qty_recent"]
    
    if ip_wo_perf_data:
        ip_perf_df = pd.DataFrame(ip_wo_perf_data, columns=ip_perf_columns)
        if not is_zone_machine:
            ip_perf_df["mach_id"] = None

        for col in ["current_wo_date", "previous_wo_date"]:
            ip_perf_df[col] = pd.to_datetime(ip_perf_df[col], errors="coerce")
            ip_perf_df.loc[ip_perf_df[col] == pd.Timestamp("1970-01-01 00:00:00"), col] = None

        merge_keys = ["zone", "mach_id", "part_cd"] if is_zone_machine else ["zone", "part_cd"]
        ip_perf_df = ip_perf_df.groupby(merge_keys, as_index=False).agg({
            "current_wo_date": "max",
            "previous_wo_date": "max",
            "total_qty": "sum",
            "total_qty_recent": "sum"
        })
    else:
        ip_perf_df = pd.DataFrame(columns=ip_perf_columns)
        if not is_zone_machine:
            ip_perf_df["mach_id"] = None

    logger.info(f"[DEBUG] ip_perf_df sample:\n{ip_perf_df.head(10)}")

    stock_df = pd.DataFrame(stock_data, columns=["part_cd", "stock"]) if stock_data else pd.DataFrame(columns=["part_cd", "stock"])
    stock_df.rename(columns={"stock": "stock_qty"}, inplace=True)

    result_df = spare_df.merge(cycle_df, on=merge_keys, how="left")
    result_df = result_df.merge(ip_perf_df, on=merge_keys, how="left")
    result_df = result_df.merge(minmax_df, on=merge_keys, how="left")
    result_df = result_df.merge(stock_df, on="part_cd", how="left")

    result_df.insert(0, "date", p_date)

    base_columns = [
        "date", "zone", "mach_id", "part_cd", "part_nm_en",
        "cycle_dt", "current_wo_date", "previous_wo_date",
        "total_qty", "total_qty_recent", "min_qty", "max_qty", "stock_qty", "rn"
    ]
    selected_columns = base_columns if is_zone_machine else [col for col in base_columns if col != "mach_id"]

    for col in selected_columns:
        if col not in result_df.columns:
            result_df[col] = None

    final_df = result_df[selected_columns].copy()
    final_df = final_df.drop_duplicates(subset=merge_keys + ["date"])

    if "cycle_dt" in final_df.columns:
        final_df["cycle_dt"] = pd.to_numeric(final_df["cycle_dt"], errors="coerce")

    final_df = final_df.where(pd.notnull(final_df), None)

    preview = final_df.head(10).values.tolist()
    for idx, row in enumerate(preview, 1):
        logger.info(f"[DEBUG] INSERT Preview {idx}: {tuple(row)}")

    result = [
        tuple(
            val.to_pydatetime() if isinstance(val, pd.Timestamp) and pd.notnull(val)
            else val if pd.notnull(val)
            else None
            for val in row
        )
        for row in final_df.itertuples(index=False, name=None)
    ]

    # Insert
    table_suffix = f"{group_by_type}_{'year' if len(p_date) == 4 else 'month'}"
    table_name = f"analysis_{table_suffix}"

    logger.info(f"[SUCCESS] final_analysis completed. Rows: {len(result)}")
    pg_v2.insert_data(schema_name="services", table_name=table_name, data=result)
# ─────────────────────────────────────────────────────────────
# 🔃 TaskGroup 생성 함수
# ─────────────────────────────────────────────────────────────
def create_python_task(task_id, func, group_by_type, p_date, dag):
    return PythonOperator(
        task_id=task_id,
        python_callable=func,
        op_kwargs={"group_by_type": group_by_type, "p_date": p_date},
        provide_context=True,
        dag=dag
    )
def create_analysis_taskgroup(dag, group_by_type, p_date):
    group_id = f"{group_by_type}_{p_date}"
    with TaskGroup(group_id=group_id, dag=dag) as tg:
        tasks = {}
        task_defs = [
            ("spare_part_top10", spare_part_top10),
            ("workorder_intervals", workorder_intervals),
            ("calculate_cycle_dt", calculate_cycle_dt),
            ("calculate_minmax", calculate_minmax),
            ("calculate_ip_wo_performance", calculate_ip_wo_performance),
            ("final_analysis", final_analysis)
        ]
        for task_name, func in task_defs:
            tasks[task_name] = create_python_task(task_name, func, group_by_type, p_date, dag)

        (
            tasks["spare_part_top10"] 
            >> tasks["workorder_intervals"] 
            >> tasks["calculate_cycle_dt"] 
            >> tasks["calculate_minmax"]
            >> tasks["calculate_ip_wo_performance"]
            >> tasks["final_analysis"] 
        )

    return tg
# ─────────────────────────────────────────────────────────────
# *️⃣ 메인 DAG 정의
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="vj_ip_sparepart_stock_analysis_apply_all",
    default_args=default_args,
    description="VJ IP SparePart Stock Analysis",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["VJ","IP","SparePart", "Analysis"]
) as dag:

    ip_rst = PythonOperator(
        task_id="ip_rst",
        python_callable=pg_v2.execute_query,
        op_args=[SQL_IP, "ip_rst", "ip_rst_data"],
    )

    latest_stock = PythonOperator(
        task_id="latest_stock",
        python_callable=pg_v1.execute_query,
        op_args=[SQL_STOCK, "stock", "stock_data"],
    )

    analysis_tables = [
        "analysis_zone_machine_month",
        "analysis_zone_machine_year",
        "analysis_zone_month",
        "analysis_zone_year"
    ]

    cleangroup = []
    for table in analysis_tables:
        clean_task = PythonOperator(
            task_id=f"clean_{table}_table",
            python_callable=pg_v2.clean_table,
            op_args=[SCHEMA_NAME, table],
            trigger_rule="all_success",
        )
        cleangroup.append(clean_task)

    taskgroups = []

    for year in range(2019, datetime.now().year + 1):
        for group_by in ["zone", "zone_machine"]:
            tg = create_analysis_taskgroup(dag, group_by, str(year))
            taskgroups.append(tg)

    start_month = date(2019, 1, 1)
    end_month = date.today().replace(day=1)

    while start_month <= end_month:
        p_date = start_month.strftime("%Y%m")
        for group_by in ["zone", "zone_machine"]:
            tg = create_analysis_taskgroup(dag, group_by, p_date)
            taskgroups.append(tg)
        start_month += relativedelta(months=1)

    for clean_task in cleangroup:
        ip_rst >> clean_task 

    for tg in taskgroups:
        cleangroup >> latest_stock >> tg 