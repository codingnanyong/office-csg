from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from app.core.database import get_session, available_databases

router = APIRouter()


@router.get("/health", summary="Readiness probe")
async def readiness_probe() -> dict[str, str]:
    """Return a simple readiness indicator."""
    return {"status": "ready"}


@router.get("/health/db", summary="Database connection test")
async def test_database_connections() -> dict:
    """Test connections to all configured databases."""
    results = {}
    
    for db_alias in available_databases():
        try:
            async with get_session(db_alias=db_alias) as session:
                # Simple query to test connection
                result = await session.execute(text("SELECT 1 FROM DUAL"))
                row = result.scalar()
                results[db_alias] = {
                    "status": "connected",
                    "test_query": "success" if row == 1 else "failed"
                }
        except Exception as e:
            results[db_alias] = {
                "status": "error",
                "error": str(e)
            }
    
    # Check if all connections are successful
    all_connected = all(
        result.get("status") == "connected" 
        for result in results.values()
    )
    
    if not all_connected:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Some database connections failed",
                "databases": results
            }
        )
    
    return {
        "status": "all_connected",
        "databases": results
    }


@router.get("/health/db/{db_alias}", summary="Test specific database connection")
async def test_database_connection(db_alias: str) -> dict:
    """Test connection to a specific database."""
    try:
        async with get_session(db_alias=db_alias) as session:
            # For Oracle, use DUAL table
            # For PostgreSQL, use SELECT 1
            if db_alias == "cmms":
                result = await session.execute(text("SELECT 1 FROM DUAL"))
            else:
                result = await session.execute(text("SELECT 1"))
            
            row = result.scalar()
            
            if row == 1:
                return {
                    "database": db_alias,
                    "status": "connected",
                    "test_query": "success"
                }
            else:
                raise HTTPException(
                    status_code=503,
                    detail=f"Database {db_alias} connection test failed"
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={
                "database": db_alias,
                "status": "error",
                "error": str(e)
            }
        )


