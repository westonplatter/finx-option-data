from xmlrpc.client import Boolean
from sqlalchemy import Integer, String, Column, Date, DateTime, Numeric, Column, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

DefaultNumericColumn = Numeric(
    precision=8, scale=2, decimal_return_scale=None, asdecimal=True
)

class StockQuote(Base):
    __tablename__ = "stock_quotes"
    id = Column(Integer, primary_key=True)
    # polygon data - all data is adjusted
    dt = Column(DateTime(timezone=True))
    ticker = Column(String(50))
    open = Column(
        Numeric(precision=8, scale=2, decimal_return_scale=None, asdecimal=True)
    )
    close = Column(DefaultNumericColumn)
    high = Column(DefaultNumericColumn)
    low = Column(DefaultNumericColumn)
    # finx derived data
    fetched = Column(Boolean, default=False)


class OptionQuote(Base):
    __tablename__ = "option_quotes"
    id = Column(Integer, primary_key=True)
    # polygon data
    dt = Column(DateTime(timezone=True))
    ticker = Column(String(50))
    open = Column(DefaultNumericColumn)
    close = Column(DefaultNumericColumn)
    high = Column(DefaultNumericColumn)
    low = Column(DefaultNumericColumn)
    volume = Column(Integer)
    pre_market = Column(DefaultNumericColumn)
    after_market = Column(DefaultNumericColumn)
    # finx derived data
    fetched = Column(Boolean, default=False)
    # finx derived data - option specific
    exp_date = Column(Date)
    dte = Column(Integer)
    strike = DefaultNumericColumn
    underlying_ticker = Column(String(50))
    delta = Column(DefaultNumericColumn)
    theta = Column(DefaultNumericColumn)
    vega = Column(DefaultNumericColumn)
    gamma = Column(DefaultNumericColumn)
    rho = Column(DefaultNumericColumn)
    option_type = Column(String(10))


class StrategyTimespreads(Base):
    __tablename__ = 'strategy_timespreads'
    id = Column(Integer, primary_key=True)
    dt = Column(DateTime(timezone=True))
    desc = Column(String(50))
    id_f = Column(Integer)
    id_b = Column(Integer)
    ticker_f = Column(String(50))
    ticker_b = Column(String(50))