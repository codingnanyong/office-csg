from sqlalchemy import Column, String, PrimaryKeyConstraint, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TagStatusSchema(Base):
    __tablename__ = "tag_status"
    __table_args__ = (
        PrimaryKeyConstraint('ws_no', 'tag_id', name='tag_status_pkey'),
        {'schema': 'services'}
    )

    ws_no = Column(String, nullable=False)
    tag_id = Column(String, nullable=False)
    op_cd = Column(String, nullable=True)
    status = Column(String, nullable=True)
    zone = Column(String, nullable=True)
    floor = Column(String, nullable=True)
    x = Column(Float, nullable=True)
    y = Column(Float, nullable=True)