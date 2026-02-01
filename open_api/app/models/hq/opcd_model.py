from sqlalchemy import Column, String, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class OpCdSchema(Base):
    __tablename__ = "opcd"
    __table_args__ = (
        PrimaryKeyConstraint('op_cd', name='dim_operations_pkey'),
        {'schema': 'services'}
    )

    op_cd = Column(String, nullable=False)
    op_name = Column(String, nullable=False)
    op_local_name = Column(String, nullable=False)
