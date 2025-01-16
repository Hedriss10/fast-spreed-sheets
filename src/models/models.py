from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class TablesProcessing(Base):
    __name__ = "tables_processing"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    status = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, autoincrement=datetime.now())

    def __repr__(self):
        return f"TablesProcessing(id={self.id!r}, name={self.name!r}, description={self.description!r}, status={self.status!r}, created_at={self.created_at!r})"

    def __str__(self):
        return f"TablesProcessing(id={self.id!r}, name={self.name!r}, description={self.description!r}, status={self.status!r}, created_at={self.created_at!r})"