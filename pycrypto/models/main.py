from sqlalchemy import BigInteger
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column, registry

main_registry = registry()


class Base(MappedAsDataclass, DeclarativeBase):
    registry = main_registry


class AppConfig(Base):
    __tablename__ = "app_config"

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    key: Mapped[str] = mapped_column(unique=True)
    value: Mapped[str]


class Bkline(Base, kw_only=True):
    __abstract__ = True

    ticker: Mapped[str] = mapped_column(primary_key=True)
    open_time: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    open: Mapped[float]
    high: Mapped[float]
    low: Mapped[float]
    close: Mapped[float]
    base_asset_volume: Mapped[float]
    close_time: Mapped[int] = mapped_column(BigInteger)
    quote_asset_volume: Mapped[float]
    number_of_trades: Mapped[int]
    taker_buy_base_asset_volume: Mapped[float]
    taker_buy_quote_asset_volume: Mapped[float]


class Klines_1s(Bkline):
    __tablename__ = "klines_1s"


class Klines_1m(Bkline):
    __tablename__ = "klines_1m"


class Klines_3m(Bkline):
    __tablename__ = "klines_3m"


class Klines_5m(Bkline):
    __tablename__ = "klines_5m"


class Klines_15m(Bkline):
    __tablename__ = "klines_15m"


class Klines_30m(Bkline):
    __tablename__ = "klines_30m"


class Klines_1h(Bkline):
    __tablename__ = "klines_1h"


class Klines_2h(Bkline):
    __tablename__ = "klines_2h"


class Klines_4h(Bkline):
    __tablename__ = "klines_4h"


class Klines_6h(Bkline):
    __tablename__ = "klines_6h"


class Klines_8h(Bkline):
    __tablename__ = "klines_8h"


class Klines_12h(Bkline):
    __tablename__ = "klines_12h"


class Klines_1d(Bkline):
    __tablename__ = "klines_1d"
