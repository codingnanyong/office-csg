from sqlalchemy import Column, String, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TagsSchema(Base):
    __tablename__ = "tags"
    __table_args__ = (
        PrimaryKeyConstraint('tag_id', name='tags_pkey'),
        {'schema': 'services'}
    )

    tag_id = Column(String, nullable=False)
    ws_no = Column(String, nullable=True)
    tag_type = Column(String, nullable=False)
