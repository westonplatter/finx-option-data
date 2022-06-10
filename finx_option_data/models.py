from sqlalchemy import Integer, String, Column, DateTime, Numeric, Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base


Base = declarative_base()

class Metric(Base):
    __tablename__ = 'metric_back_front_vol'
    id = Column(Integer, primary_key=True)
    dt = Column(DateTime)
    key = Column(String)
    value = Numeric(precision=8, scale=4, decimal_return_scale=None, asdecimal=True)
    version = Column(Integer)

class ApiKey(Base):
    __tablename__ = "api_key"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    value_json = Column(JSONB)
    value_json = Column(String)