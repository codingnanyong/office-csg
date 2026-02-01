"""SQLAlchemy model for the `sensor` table."""

from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.orm import relationship

from app.core.database import Base


class Sensor(Base):
    """Represents a row in the `sensor` table."""

    __tablename__ = "sensor"
    __table_args__ = {"schema": "public"}

    sensor_id = Column(String(20), primary_key=True, nullable=False)
    device_id = Column(String(20), ForeignKey("device.device_id", ondelete="CASCADE"), nullable=False)
    mach_id = Column(String(30), ForeignKey("machine.mach_id", ondelete="CASCADE"), nullable=False)
    company_cd = Column(String(10), nullable=False)
    name = Column(String(30), nullable=True)
    addr = Column(String(50), nullable=True)
    topic = Column(String(100), nullable=True)
    descn = Column(String(200), nullable=True)
    upd_dt = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"Sensor(sensor_id='{self.sensor_id}', name='{self.name}')"

