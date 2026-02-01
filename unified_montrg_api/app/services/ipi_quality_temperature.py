"""Service layer for IPI Quality Temperature data operations."""

from typing import List, Dict, Any

from sqlalchemy import select, exists, and_, or_

from app.core.database import get_session
from app.models.ipi_quality_temperature import (
    IpiQualityTemperature,
    IpiQualityTemperatureDetail,
)
from app.schemas.ipi_quality_temperature import (
    IpiQualityTemperatureItem,
    IpiQualityTemperatureDetailItem,
)


async def fetch_ipi_quality_temperature(
    osnd_id: int,
) -> List[Dict[str, Any]]:
    """Fetch IPI Quality Temperature data with detail records from gold.ipi_temperature_matching tables
    
    Args:
        osnd_id: filter by defect ID (required)
    
    Returns:
        List of dictionaries with main record and detail records grouped by osnd_id
        (reason_cd <> 'good' condition is always applied, and records must have both L and U temp types)
    """
    async with get_session(db_alias="montrg") as session:
        # Create correlated subqueries for detail records
        # Check if L type detail exists with seq_no
        detail_l = exists(
            select(1).where(
                and_(
                    IpiQualityTemperatureDetail.osnd_id == IpiQualityTemperature.osnd_id,
                    IpiQualityTemperatureDetail.temp_type == 'L',
                    IpiQualityTemperatureDetail.seq_no.isnot(None),
                )
            )
        )
        
        # Check if U type detail exists with seq_no
        detail_u = exists(
            select(1).where(
                and_(
                    IpiQualityTemperatureDetail.osnd_id == IpiQualityTemperature.osnd_id,
                    IpiQualityTemperatureDetail.temp_type == 'U',
                    IpiQualityTemperatureDetail.seq_no.isnot(None),
                )
            )
        )
        
        # Build main query with filters
        query = select(IpiQualityTemperature).where(
            and_(
                IpiQualityTemperature.reason_cd != 'good',
                IpiQualityTemperature.osnd_id == osnd_id,
                detail_l,
                detail_u,
            )
        )
        
        # Order by osnd_id descending
        query = query.order_by(IpiQualityTemperature.osnd_id.desc())
        
        # Execute main query
        result = await session.execute(query)
        main_records = result.scalars().all()
        
        if not main_records:
            return []
        
        # Get osnd_id and osnd_dt pairs for detail query
        # FDW Environment Compatibility: Explicit conditions for composite key matching
        osnd_keys = [(r.osnd_id, r.osnd_dt) for r in main_records]
        
        if len(osnd_keys) == 0:
            return []
        
        # Build OR conditions for composite key matching
        # For FDW compatibility, use explicit conditions
        conditions = []
        for osnd_id, osnd_dt in osnd_keys:
            conditions.append(
                and_(
                    IpiQualityTemperatureDetail.osnd_id == osnd_id,
                    IpiQualityTemperatureDetail.osnd_dt == osnd_dt,
                )
            )
        
        detail_query = select(IpiQualityTemperatureDetail).where(
            or_(*conditions) if len(conditions) > 1 else conditions[0]
        ).order_by(
            IpiQualityTemperatureDetail.osnd_id.desc(),
            IpiQualityTemperatureDetail.seq_no.asc()
        )
        
        detail_result = await session.execute(detail_query)
        detail_records = detail_result.scalars().all()
        
        # Group details by (osnd_id, osnd_dt)
        details_by_key: Dict[tuple, List[Dict[str, Any]]] = {}
        for detail in detail_records:
            key = (detail.osnd_id, detail.osnd_dt)
            if key not in details_by_key:
                details_by_key[key] = []
            details_by_key[key].append({
                "temp_type": detail.temp_type,
                "measurement_time": detail.measurement_time,
                "temperature": detail.temperature,
                "seq_no": detail.seq_no
            })
        
        # Build result structure with details
        result_list = []
        for main_record in main_records:
            key = (main_record.osnd_id, main_record.osnd_dt)
            details = details_by_key.get(key, [])
            
            result_list.append({
                "osnd_id": main_record.osnd_id,
                "osnd_dt": main_record.osnd_dt,
                "machine_cd": main_record.machine_cd,
                "mold_id": main_record.mold_id,
                "reason_cd": main_record.reason_cd,
                "size_cd": main_record.size_cd,
                "station": main_record.station,
                "station_rl": main_record.station_rl,
                "lr_cd": main_record.lr_cd,
                "osnd_bt_qty": main_record.osnd_bt_qty,
                "details": details,
            })
        
        return result_list

