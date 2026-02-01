from sqlalchemy import Column, String, PrimaryKeyConstraint, TIMESTAMP, Interval
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TagHisSchema(Base):
    __tablename__ = "tag_his"
    __table_args__ = (
        PrimaryKeyConstraint('ws_no', 'tag_id','now', name='tag_his_pkey'),
        {'schema': 'services'}
    )

    ws_no = Column(String, nullable=False)
    tag_id = Column(String, nullable=False)
    op_cd = Column(String, nullable=True)
    status = Column(String, nullable=True)
    zone = Column(String, nullable=True)
    floor = Column(String, nullable=True)
    now = Column(TIMESTAMP, nullable=False)
    next = Column(TIMESTAMP, nullable=True)
    diff = Column(Interval, nullable=True)