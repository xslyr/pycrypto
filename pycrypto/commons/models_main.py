from sqlalchemy import BigInteger
from sqlalchemy.orm import Mapped, mapped_as_dataclass, mapped_column, registry

main_registry = registry()


@mapped_as_dataclass(main_registry)
class AppConfig:
    __tablename__ = "app_config"

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    key: Mapped[str] = mapped_column(unique=True)
    value: Mapped[str]


class Bkline:
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


@mapped_as_dataclass(main_registry)
class Klines_1s(Bkline):
    __tablename__ = "klines_1s"


@mapped_as_dataclass(main_registry)
class Klines_1m(Bkline):
    __tablename__ = "klines_1m"


@mapped_as_dataclass(main_registry)
class Klines_3m(Bkline):
    __tablename__ = "klines_3m"


@mapped_as_dataclass(main_registry)
class Klines_5m(Bkline):
    __tablename__ = "klines_5m"


@mapped_as_dataclass(main_registry)
class Klines_15m(Bkline):
    __tablename__ = "klines_15m"


@mapped_as_dataclass(main_registry)
class Klines_30m(Bkline):
    __tablename__ = "klines_30m"


@mapped_as_dataclass(main_registry)
class Klines_1h(Bkline):
    __tablename__ = "klines_1h"


@mapped_as_dataclass(main_registry)
class Klines_2h(Bkline):
    __tablename__ = "klines_2h"


@mapped_as_dataclass(main_registry)
class Klines_4h(Bkline):
    __tablename__ = "klines_4h"


@mapped_as_dataclass(main_registry)
class Klines_6h(Bkline):
    __tablename__ = "klines_6h"


@mapped_as_dataclass(main_registry)
class Klines_8h(Bkline):
    __tablename__ = "klines_8h"


@mapped_as_dataclass(main_registry)
class Klines_12h(Bkline):
    __tablename__ = "klines_12h"


@mapped_as_dataclass(main_registry)
class Klines_1d(Bkline):
    __tablename__ = "klines_1d"
