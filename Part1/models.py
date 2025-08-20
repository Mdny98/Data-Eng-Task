from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class City(Base):
    __tablename__ = "cities"
    name = Column(String, primary_key=True, index=True)
    country_code = Column(String, nullable=False)
