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

from finance.enumerations import Frequency
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
class AlpacaFrequencyError(AlpacaError): pass
class AlpacaFrequencyByError(AlpacaFrequencyError): pass
class AlpacaFrequencySpanError(AlpacaFrequencyError): pass


class AlpacaParsers:
    pagination = lambda string: str(string) if string != "None" else None
    expire = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
    history = lambda string: pd.to_datetime(string, utc=True)
    strike = lambda string: np.round(float(string), 2)


@dataclass(frozen=True)
class AlpacaColumn: name: str; code: str; parser: Callable
class AlpacaColumns(metaclass=CollectionMeta):
    datetime = AlpacaColumn("datetime", "t", lambda string: pd.to_datetime(string, utc=True))
    volume = AlpacaColumn("volume", "v", np.int64)
    size = AlpacaColumn("size", "s", np.float32)
    supply = AlpacaColumn("supply", "as", np.float32)
    demand = AlpacaColumn("demand", "bs", np.float32)
    trade = AlpacaColumn("trade", "p", np.float32)
    bid = AlpacaColumn("bid", "bp", np.float32)
    ask = AlpacaColumn("ask", "ap", np.float32)
    open = AlpacaColumn("open", "o", np.float32)
    close = AlpacaColumn("close", "c", np.float32)
    high = AlpacaColumn("high", "h", np.float32)
    low = AlpacaColumn("low", "l", np.float32)


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
    def records(json, *args, **kwargs):
        columns = list(dict(AlpacaColumns).values())
        for product, contents in json.items():
            assert isinstance(contents, (list, dict))
            if isinstance(contents, dict): contents = [contents]
            for mapping in contents:
                assert isinstance(mapping, dict)
                mapping = {column.name: column.parser(mapping[column.code]) for column in columns if column.code in mapping.keys()}
                yield {"product": product} | mapping

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



