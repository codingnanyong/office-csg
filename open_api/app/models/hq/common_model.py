from sqlalchemy import Column, String, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CommonSchema(Base):
    __tablename__ = "common"
    __table_args__ = (
        PrimaryKeyConstraint('com_div', 'com_cd', name='common_pkey'),
        {'schema': 'services'}
    )

    com_div = Column(String, primary_key=True, index=True)
    com_cd = Column(String, primary_key=True, index=True)
    com_name = Column(String, nullable=False)