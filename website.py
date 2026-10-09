# -*- coding: utf-8 -*-
"""
Created on Mon Oct 5 2026
@name:   Alpaca Website Objects
@author: Jack Kirby Cook
@file:   alpaca\website.py

"""

from abc import ABC
from typing import Callable
from dataclasses import dataclass
from types import SimpleNamespace

from finance.enumerations import Frequency
from finance.reporting import Results
from webscraping.weburl import WebURL
from webscraping.webpages import WebJSONPage
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaDownloadPage", "AlpacaUploadPage", "AlpacaDownloadURL", "AlpacaUploadURL", "AlpacaDownloader", "AlpacaField"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaError(Exception): pass
class AlpacaFrequencyError(AlpacaError): pass
class AlpacaFrequencyByError(AlpacaFrequencyError): pass
class AlpacaFrequencySpanError(AlpacaFrequencyError): pass


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: Callable

@dataclass(frozen=False)
class AlpacaFrequency:
    by: Frequency; code: str; span: list[int]

    def __call__(self, span):
        if span not in self.span: raise AlpacaFrequencySpanError()
        return f"{int(span)}{str(self.code)}"


frequency_months = AlpacaFrequency(Frequency.MONTHLY, "M", list(value for value in range(1, 13) if not 12 % value))
frequency_minutes = AlpacaFrequency(Frequency.MINUTELY, "T", list(range(1, 59 + 1)))
frequency_hours = AlpacaFrequency(Frequency.HOURLY, "H", list(range(1, 23 + 1)))
frequency_weeks = AlpacaFrequency(Frequency.WEEKLY, "W", [1])
frequency_days = AlpacaFrequency(Frequency.DAILY, "D", [1])
frequencies = [frequency_months, frequency_weeks, frequency_days, frequency_hours, frequency_minutes]
frequencies = {frequency.by: frequency for frequency in frequencies}


class AlpacaURL(WebURL):
    @staticmethod
    def frequency(*args, frequency, **kwargs):
        if frequency.by not in frequencies.keys(): raise AlpacaFrequencyByError()
        return frequencies[frequency.by](frequency.span)

    @staticmethod
    def history(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if pagination is not None else {}
    @staticmethod
    def headers(*args, authenticator, **kwargs): return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaUploadURL(AlpacaURL, headers={"accept": "application/json", "content-type": "application/json"}): pass
class AlpacaDownloadURL(AlpacaURL, headers={"accept": "application/json"}): pass


class AlpacaPage(WebJSONPage):
    def __init_subclass__(cls, /, url=None, data=None, **kwargs):
        super().__init_subclass__(**kwargs)
        namespace = SimpleNamespace(url=url, data=data)
        cls.__namespace__ = namespace

    def __init__(self, *args, authenticator, **kwargs):
        super().__init__(*args, **kwargs)
        self.__authenticator = authenticator

    def url(self, *args, **kwargs):
        parameters = dict(authenticator=self.authenticator)
        return self.namespace.url(*args, **parameters, **kwargs)

    def data(self, json, *args, **kwargs):
        if self.namespace.data is None: return None
        return self.namespace.data(json, *args, **kwargs)

    @property
    def namespace(self): return type(self).__namespace__
    @property
    def authenticator(self): return self.__authenticator


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



