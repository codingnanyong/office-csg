from sqlalchemy import Column, String, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ToolingSchema(Base):
    __tablename__ = "tooling"
    __table_args__ = (
        PrimaryKeyConstraint("purc_no", "uptl_no", name="tooling_pkey"),
        {"schema": "services"} 
    )

    purc_no = Column(String, nullable=False)
    uptl_no = Column(String, nullable=False)
    parent_uptl_no = Column(String, nullable=True)
    part_cd = Column(String, nullable=True)
    part_name = Column(String, nullable=True)
    process_cd = Column(String, nullable=True)
    process_name = Column(String, nullable=True)
    tool_cd = Column(String, nullable=True)
    tool_name = Column(String, nullable=True)
    tool_size = Column(String, nullable=True)
    status = Column(String, nullable=True)
    loc_cd = Column(String, nullable=True)
    loc_name = Column(String, nullable=True)
