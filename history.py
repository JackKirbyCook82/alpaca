# -*- coding: utf-8 -*-
"""
Created on Thurs Mar 26 2026
@name:   Alpaca History Objects
@author: Jack Kirby Cook
@file:   alpaca/history.py

"""

import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict

from finance.enumerations import Instrument, Frequency
from finance.reporting import Results
from finance.osi import OSI
from webscraping.webpages import WebJSONPage, WebStream
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaBarsDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


frequency_mapping = {Frequency.MINUTELY: "T", Frequency.HOURLY: "H", Frequency.DAILY: "D", Frequency.WEEKLY: "W", Frequency.MONTHLY: "M"}
frequency_parser = lambda frequency: f"{int(frequency.duration)}{frequency_mapping[frequency.by]}"
pagination_parser = lambda string: str(string) if string != "None" else None
history_parser = lambda string: pd.to_datetime(string, utc=True).date()


class AlpacaHistoryURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaBarsURL(AlpacaHistoryURL, domain="https://data.alpaca.markets", path=["v2"], parameters={"limit": 10000}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        products = cls.products(*args, **kwargs)
        frequency = cls.frequency(*args, **kwargs)
        history = cls.history(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return products | frequency | history | pagination

    @staticmethod
    def products(*args, products, **kwargs): raise NotImplementedError()
    @staticmethod
    def frequency(*args, frequency, **kwargs): return {"timeframe": frequency_parser(frequency)}
    @staticmethod
    def history(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs):
        if pagination is not None: return {"page_token": str(pagination)}
        else: return {}


class AlpacaStockBarsURL(AlpacaBarsURL, path=["stocks", "bars"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionBarsURL(AlpacaBarsURL, path=["v1beta1", "options", "bars"]):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaHistoryData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, optional=True): pass


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: callable


class AlpacaHistoryPage(WebJSONPage, ABC): pass
class AlpacaBarsPage(AlpacaHistoryPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("open", "o", np.float32), AlpacaField("close", "c", np.float32), AlpacaField("high", "h", np.float32), AlpacaField("low", "l", np.float32), AlpacaField("adjusted", "vw", np.float32)]
        fields = fields + [AlpacaField("date", "t", history_parser), AlpacaField("volume", "v", np.int64)]
        parser = lambda mapping: {field.name: field.parser(mapping[field.code]) for field in self.fields if field.code in mapping.keys()}
        self.__fields = fields
        self.__parser = parser

    def __call__(self, *args, products, frequency, history, **kwargs):
        parameters = dict(products=products, frequency=frequency, history=history, authenticator=self.authenticator)
        records = self.bars(**parameters)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def bars(self, *args, pagination=None, **kwargs):
        url = AlpacaBarsURL(*args, pagination=pagination, **kwargs)
        json = self.load(url)
        records = [{"product": product} | self.parser(mapping) for product, contents in json["bars"].items() for mapping in contents]
        datas = AlpacaHistoryData(json, *args, **kwargs)
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.bars(*args, pagination=pagination, **kwargs)

    @property
    def fields(self): return self.__fields
    @property
    def parser(self): return self.__parser


class AlpacaHistoryDownloader(WebStream, Results, Logging, ABC):
    @abstractmethod
    def downloader(self, products, /, **kwargs): pass
    @abstractmethod
    def download(self, products, /, **kwargs): pass


class AlpacaBarsDownloader(AlpacaHistoryDownloader, ABC, page=AlpacaBarsPage):
    def __init_subclass__(cls, /, instrument, **kwargs): cls.__instrument__ = instrument
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        bars = self.download(products, **kwargs)
        return bars

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + self.capacity] for index in range(0, len(products), self.capacity)]
        for products in products:
            scope = self.scope(products, instrument=type(self).instrument)
            bars = self.page(products=products, **kwargs)
            if bars is None or bool(bars.empty): continue
            results = self.results(scope=scope, size=len(bars))
            self.console("Downloaded", results)
            yield bars

    @property
    def instrument(self): return type(self).__instrument__


class AlpacaStockBarsDownloader(AlpacaBarsDownloader, instrument=Instrument.STOCK):
    def download(self, products, /, **kwargs):
        bars = self.downloader(products, **kwargs)
        bars = pd.concat(list(bars), axis=0)
        bars["date"] = pd.to_datetime(bars["date"])
        bars = bars.sort_values(by=["product", "date"], ascending=[True, False], inplace=False)
        bars = bars.rename(columns={"product": "ticker"})
        bars = bars.reset_index(drop=True, inplace=False)
        return bars


class AlpacaOptionBarsDownloader(AlpacaBarsDownloader, instrument=Instrument.OPTION):
    def download(self, products, /, **kwargs):
        bars = self.downloader(products, **kwargs)
        bars = pd.concat(list(bars), axis=0)
        bars["date"] = pd.to_datetime(bars["date"])
        bars = bars.sort_values(by=["product", "date"], ascending=[True, False], inplace=False)
        bars = bars.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(bars["osi"].map(OSI).map(asdict), index=bars.index)
        bars = pd.concat([bars, contracts], axis=1)
        bars = bars.reset_index(drop=True, inplace=False)
        return bars



