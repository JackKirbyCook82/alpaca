# -*- coding: utf-8 -*-
"""
Created on Mon Oct 5 2026
@name:   Alpaca Website Objects
@author: Jack Kirby Cook
@file:   alpaca\website.py

"""

import numpy as np
import pandas as pd
from abc import ABC
from typing import Callable
from dataclasses import dataclass
from datetime import datetime as Datetime

from finance.enumerations import Frequency, Instrument, Dataset, Price
from finance.reporting import Results
from webscraping.weburl import WebURL
from webscraping.webpages import WebJSONPage
from support.meta import CollectionMeta
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaDownloadPage", "AlpacaUploadPage", "AlpacaDownloadURL", "AlpacaUploadURL", "AlpacaDownloader", "AlpacaParsers", "AlpacaColumns"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaError(Exception): pass
class AlpacaColumnError(AlpacaError): pass
class AlpacaFrequencyError(AlpacaError): pass
class AlpacaFrequencyByError(AlpacaFrequencyError): pass
class AlpacaFrequencySpanError(AlpacaFrequencyError): pass


class AlpacaParsers:
    pagination = lambda string: str(string) if string != "None" else None
    expire = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
    history = lambda string: pd.to_datetime(string, utc=True)
    strike = lambda string: np.round(float(string), 2)


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: Callable
class AlpacaFields(metaclass=CollectionMeta):
    datetime = AlpacaField("datetime", "t", lambda string: pd.to_datetime(string, utc=True))
    volume = AlpacaField("volume", "v", np.int64)
    size = AlpacaField("size", "s", np.float32)
    supply = AlpacaField("supply", "as", np.float32)
    demand = AlpacaField("demand", "bs", np.float32)
    trade = AlpacaField("trade", "p", np.float32)
    bid = AlpacaField("bid", "bp", np.float32)
    ask = AlpacaField("ask", "ap", np.float32)
    open = AlpacaField("open", "o", np.float32)
    close = AlpacaField("close", "c", np.float32)
    high = AlpacaField("high", "h", np.float32)
    low = AlpacaField("low", "l", np.float32)

    def __new__(cls, data, columns):
        generator = cls.generator(data, columns)
        records = list(generator)
        return records

    @classmethod
    def generator(cls, data, columns):
        fields = [field for column, field in dict(cls).items() if column in columns]
        for product, contents in data.items():
            assert isinstance(contents, (list, dict))
            if isinstance(contents, dict): contents = [contents]
            for mapping in contents:
                assert isinstance(mapping, dict)
                mapping = {field.name: field.parser(mapping[field.code]) for field in fields if field.code in mapping.keys()}
                yield {"product": product} | mapping


@dataclass(frozen=True)
class AlpacaColumn: dataset: Dataset; price: Price; instrument: Instrument; values: list[str]
class AlpacaColumns(metaclass=CollectionMeta):
    bars_trade_stock = AlpacaColumn(Dataset.BARS, Price.TRADE, Instrument.STOCK, ["ticker", "datetime", "open", "close", "high", "low", "volume"])
    bars_trade_option = AlpacaColumn(Dataset.BARS, Price.TRADE, Instrument.OPTION, ["ticker", "expire", "option", "strike", "datetime", "open", "close", "high", "low", "volume"])
    ticks_trade_stock = AlpacaColumn(Dataset.TICKS, Price.TRADE, Instrument.STOCK, ["ticker", "datetime", "trade", "size"])
    ticks_trade_option = AlpacaColumn(Dataset.TICKS, Price.TRADE, Instrument.OPTION, ["ticker", "expire", "option", "strike", "datetime", "trade", "size"])
    spot_quote_stock = AlpacaColumn(Dataset.SPOT, Price.QUOTE, Instrument.STOCK, ["ticker", "datetime", "ask", "bid", "supply", "demand"])
    spot_quote_option = AlpacaColumn(Dataset.SPOT, Price.QUOTE, Instrument.OPTION, ["ticker", "expire", "option", "strike", "datetime", "ask", "bid", "supply", "demand"])
    spot_trade_stock = AlpacaColumn(Dataset.SPOT, Price.TRADE, Instrument.STOCK, ["ticker", "datetime", "trade", "size"])
    spot_trade_option = AlpacaColumn(Dataset.SPOT, Price.TRADE, Instrument.OPTION, ["ticker", "expire", "option", "strike", "datetime", "trade", "size"])

    def __new__(cls, dataset, price, instrument):
        key = lambda column: (column.dataset, column.price, column.instrument)
        columns = {key(column): column for column in dict(cls).values()}
        try: return columns[dataset, price, instrument].values
        except KeyError: raise AlpacaColumnError


@dataclass(frozen=True)
class AlpacaFrequency: by: Frequency; code: str; span: list[int]
class AlpacaFrequencies(metaclass=CollectionMeta):
    months = AlpacaFrequency(Frequency.MONTHLY, "M", list(value for value in range(1, 13) if not 12 % value))
    minutes = AlpacaFrequency(Frequency.MINUTELY, "T", list(range(1, 59 + 1)))
    hours = AlpacaFrequency(Frequency.HOURLY, "H", list(range(1, 23 + 1)))
    weeks = AlpacaFrequency(Frequency.WEEKLY, "W", [1])
    days = AlpacaFrequency(Frequency.DAILY, "D", [1])

    def __new__(cls, frequency):
        by, span = frequency.by, frequency.span
        frequencies = {frequency.by: frequency for frequency in dict(cls).values()}
        if by not in frequencies.keys(): raise AlpacaFrequencyByError()
        if span not in frequencies[by].span: raise AlpacaFrequencySpanError()
        return f"{int(span)}{str(frequencies[by].code)}"


class AlpacaURL(WebURL):
    @staticmethod
    def headers(*args, authenticator, **kwargs): return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if pagination is not None else {}
    @staticmethod
    def history(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def frequency(*args, frequency, **kwargs): return {"timeframe": AlpacaFrequencies(frequency)}

    @staticmethod
    def expires(*args, expires=None, **kwargs):
        if expires is not None: return {"expiration_date_gte": str(expires.minimum.strftime("%Y-%m-%d")), "expiration_date_lte": str(expires.maximum.strftime("%Y-%m-%d"))}
        else: return {}

    @staticmethod
    def strikes(*args, strikes=None, **kwargs):
        if strikes is not None: return {"strike_price_gte": str(strikes.minimum), "strike_price_lte": str(strikes.maximum)}
        else: return {}


class AlpacaUploadURL(AlpacaURL, headers={"accept": "application/json", "content-type": "application/json"}): pass
class AlpacaDownloadURL(AlpacaURL, headers={"accept": "application/json"}): pass


class AlpacaPage(WebJSONPage):
    def __init_subclass__(cls, /, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = getattr(cls, "__namespace__", {})
        instrument = kwargs.get("instrument", namespace.get("instrument", None))
        data = kwargs.get("data", namespace.get("data", None))
        url = kwargs.get("url", namespace.get("url", None))
        cls.__namespace__ = dict(instrument=instrument, data=data, url=url)

    def __init__(self, *args, authenticator, **kwargs):
        super().__init__(*args, **kwargs)
        self.__instrument = self.namespace["instrument"]
        self.__authenticator = authenticator

    def url(self, *args, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        return self.namespace["url"](*args, **parameters, **kwargs)

    def data(self, json, *args, **kwargs):
        if self.namespace["data"] is None: return None
        return self.namespace["data"](json, *args, **kwargs)

    @staticmethod
    def records(data, *args, columns, **kwargs): return AlpacaFields(data, columns=columns)

    @property
    def namespace(self): return type(self).__namespace__
    @property
    def authenticator(self): return self.__authenticator
    @property
    def instrument(self): return self.__instrument


class AlpacaUploadPage(AlpacaPage): pass
class AlpacaDownloadPage(AlpacaPage): pass


class AlpacaDownloader(Results, Logging, ABC):
    def __init_subclass__(cls, /, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = getattr(cls, "__namespace__", {})
        instrument = kwargs.get("instrument", namespace.get("instrument", None))
        pages = kwargs.get("pages", namespace.get("pages", {}))
        page = kwargs.get("page", namespace.get("page", None))
        url = kwargs.get("url", namespace.get("url", None))
        cls.__namespace__ = dict(instrument=instrument, pages=pages, page=page, url=url)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__pages = {key: page(*args, **kwargs) for key, page in self.namespace["pages"].items()}
        self.__page = self.namespace["page"](*args, **kwargs) if self.namespace["page"] is not None else None
        self.__instrument = self.namespace["instrument"]

    def scope(self, products, **kwargs):
        if self.instrument is None: raise ValueError(self.instrument)
        return super().scope(products, instrument=self.instrument)

    @property
    def namespace(self): return type(self).__namespace__
    @property
    def instrument(self): return self.__instrument
    @property
    def pages(self): return self.__pages
    @property
    def page(self): return self.__page



