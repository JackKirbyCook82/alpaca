# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 2025
@name:   Aplaca History Objects
@author: Jack Kirby Cook

"""

from datetime import datetime as Datetime

from finance.variables import Querys
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging
from support.custom import SliceOrderedDict as SODict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaHistoryURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaBarsURL(AlpacaHistoryURL, domain="https://data.alpaca.markets", path=["v2", "stocks", "bars"], parameters={"feed": "sip", "limit": "10000"}):
    @staticmethod
    def parameters(*args, ticker, dates, **kwargs):
        assert isinstance(ticker, str)
        return {"symbols": str(ticker), "start": Datetime.strptime(dates.minimum, "%Y-%m-%d"), "end": Datetime.strptime(dates.maximum, "%Y-%m-%d")}


class AlpacaBarsData(WebJSON.Mapping, key="bars", locator="//bars", multiple=False, optional=False):
    def execute(self, *args, **kwargs):
        pass


class AlpacaMarketPage(WebJSONPage):
    def execute(self, *args, **kwargs):
        url = AlpacaBarsURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaBarsData(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents


class AlpacaBarsDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaMarketPage(*args, **kwargs)

    def execute(self, symbols, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return

    def download(self, *args, **kwargs):
        pass

    @staticmethod
    def querys(querys, querytype):
        assert isinstance(querys, (list, dict, querytype))
        assert all([isinstance(query, querytype) for query in querys]) if isinstance(querys, (list, dict)) else True
        if isinstance(querys, querytype): querys = [querys]
        elif isinstance(querys, dict): querys = SODict(querys)
        else: querys = list(querys)
        return querys

    @property
    def page(self): return self.__page


