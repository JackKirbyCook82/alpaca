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
from types import SimpleNamespace
from datetime import datetime as Datetime

from finance.enumerations import Frequency
from finance.reporting import Results
from webscraping.weburl import WebURL
from webscraping.webpages import WebJSONPage
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaDownloadPage", "AlpacaUploadPage", "AlpacaDownloadURL", "AlpacaUploadURL", "AlpacaDownloader", "AlpacaParsers"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaError(Exception): pass
class AlpacaFieldError(AlpacaError): pass
class AlpacaFrequencyError(AlpacaError): pass
class AlpacaFrequencyByError(AlpacaFrequencyError): pass
class AlpacaFrequencySpanError(AlpacaFrequencyError): pass


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: Callable

@dataclass(frozen=True)
class AlpacaParser: name: str; parser: Callable

@dataclass(frozen=True)
class AlpacaFrequency: by: Frequency; code: str; span: list[int]


class AlpacaFields:
    datetime = AlpacaField("datetime", "t", lambda string: pd.to_datetime(string, utc=True))
    volume = AlpacaField("volume", "v", np.int64)
    trade = AlpacaField("trade", "p", np.float32)
    bid = AlpacaField("bid", "bp", np.float32)
    ask = AlpacaField("ask", "ap", np.float32)
    supply = AlpacaField("supply", "as", np.float32)
    demand = AlpacaField("demand", "bs", np.float32)
    open = AlpacaField("open", "o", np.float32)
    close = AlpacaField("close", "c", np.float32)
    high = AlpacaField("high", "h", np.float32)
    low = AlpacaField("low", "l", np.float32)

class AlpacaParsers:
    pagination = lambda string: str(string) if string != "None" else None
    expire = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
    history = lambda string: pd.to_datetime(string, utc=True)
    strike = lambda string: np.round(float(string), 2)

class AlpacaFrequencies:
    months = AlpacaFrequency(Frequency.MONTHLY, "M", list(value for value in range(1, 13) if not 12 % value))
    minutes = AlpacaFrequency(Frequency.MINUTELY, "T", list(range(1, 59 + 1)))
    hours = AlpacaFrequency(Frequency.HOURLY, "H", list(range(1, 23 + 1)))
    weeks = AlpacaFrequency(Frequency.WEEKLY, "W", [1])
    days = AlpacaFrequency(Frequency.DAILY, "D", [1])

    def __new__(cls, frequency):
        by, span = frequency.by, frequency.span
        frequencies = [cls.months, cls.weeks, cls.days, cls.hours, cls.minutes]
        frequencies = {frequency.by: frequency for frequency in frequencies}
        if by not in frequencies.keys(): raise AlpacaFrequencyByError()
        if span not in frequencies[by].span: raise AlpacaFrequencySpanError()
        return f"{int(span)}{str(frequencies[by].code)}"


class AlpacaURL(WebURL):
    @staticmethod
    def history(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def frequency(*args, frequency, **kwargs): return {"timeframe": AlpacaFrequencies(frequency)}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if pagination is not None else {}
    @staticmethod
    def headers(*args, authenticator, **kwargs): return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}

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
    def __init_subclass__(cls, /, url=None, data=None, fields=None, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = SimpleNamespace(url=url, data=data, fields=fields)
        cls.__namespace__ = namespace

    def __init__(self, *args, authenticator, **kwargs):
        super().__init__(*args, **kwargs)
        try: self.__fields = [getattr(AlpacaFields, field) for field in self.namespace.fields]
        except AttributeError: raise AlpacaFieldError()
        self.__authenticator = authenticator

    def url(self, *args, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        return self.namespace.url(*args, **parameters, **kwargs)

    def data(self, json, *args, **kwargs):
        if self.namespace.data is None: return None
        return self.namespace.data(json, *args, **kwargs)

    def records(self, json, *args, **kwargs):
        if self.namespace.fields is None: return
        for product, contents in json.items():
            assert isinstance(contents, (list, dict))
            if isinstance(contents, dict): contents = [contents]
            for mapping in contents:
                assert isinstance(mapping, dict)
                mapping = {field.name: field.parser(mapping[field.code]) for field in self.fields if field.code in mapping.keys()}
                yield {"product": product} | mapping

    @property
    def namespace(self): return type(self).__namespace__
    @property
    def authenticator(self): return self.__authenticator
    @property
    def fields(self): return self.__fields


class AlpacaUploadPage(AlpacaPage): pass
class AlpacaDownloadPage(AlpacaPage): pass


class AlpacaDownloader(Results, Logging, ABC):
    def __init_subclass__(cls, /, page=None, columns=None, instrument=None, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = dict(pages=kwargs.get("pages", {}), page=page, columns=columns, instrument=instrument)
        cls.__namespace__ = SimpleNamespace(**namespace)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        namespace = type(self).__namespace__
        self.__pages = {key: page(*args, **kwargs) for key, page in namespace.pages.items()}
        self.__page = namespace.page(*args, **kwargs) if namespace.page is not None else None
        self.__instrument = namespace.instrument
        self.__columns = namespace.columns

    def scope(self, products, **kwargs):
        if self.instrument is None: raise ValueError(self.instrument)
        return super().scope(products, instrument=self.instrument)

    @property
    def instrument(self): return self.__instrument
    @property
    def columns(self): return self.__columns
    @property
    def pages(self): return self.__pages
    @property
    def page(self): return self.__page



