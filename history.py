# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 2025
@name:   Aplaca History Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from datetime import timezone as Timezone
from datetime import datetime as Datetime

from finance.variables import Querys
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging
from support.custom import SliceOrderedDict as SODict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaBarsDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


date_parser = lambda value: Datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=Timezone.utc).date()
price_parsers = {code: (key, lambda value: np.float32(value)) for key, code in {"open": "o", "close": "c", "high": "h", "low": "l", "price": "vw"}.items()}
size_parsers = {code: (key, lambda value: np.int64(value)) for key, code in {"volume": "v"}.items()}
date_parsers = {code: (key, lambda value: date_parser(value)) for key, code in {"date": "t"}.items()}
bars_parser = lambda mapping: {key: function(mapping[code]) for code, (key, function) in (price_parsers | size_parsers | date_parsers).items() if code in mapping.keys()}


class AlpacaHistoryURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}

class AlpacaBarsURL(AlpacaHistoryURL, domain="https://data.alpaca.markets", path=["v2", "stocks"], parameters={"timeframe": "1Day", "feed": "sip", "limit": "10000"}):
    @staticmethod
    def path(*args, ticker, **kwargs): return [str(ticker), "bars"]
    @staticmethod
    def parameters(*args, dates, **kwargs): return {"start": dates.minimum.strftime("%Y-%m-%d"), "end": dates.maximum.strftime("%Y-%m-%d")}


class AlpacaHistoryData(WebJSON, multiple=False, optional=False):
    class Ticker(WebJSON.Text, key="ticker", locator="//symbol", parser=str, multiple=False, optional=False): pass
    class Bars(WebJSON.Mapping, key="bars", locator="//bars", parser=bars_parser, multiple=True, optional=False): pass


class AlpacaHistoryPage(WebJSONPage):
    def execute(self, *args, **kwargs):
        url = AlpacaBarsURL(*args, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaHistoryData(self.json, *args, **kwargs)
        contents = [data(*args, **kwargs) for data in datas["bars"]]
        dataframe = pd.DataFrame.from_records(contents)
        dataframe["ticker"] = datas["ticker"](*args, **kwargs)
        dataframe = dataframe.sort_values("date", axis=0, ascending=True, inplace=False)
        return dataframe


class AlpacaBarsDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, api, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaHistoryPage(*args, **kwargs)
        self.__api = api

    def execute(self, symbols, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol in iter(symbols):
            parameters = dict(ticker=str(symbol.ticker), api=self.api)
            bars = self.download(*args, **parameters, **kwargs)
            assert isinstance(bars, pd.DataFrame)
            if isinstance(symbols, dict):
                function = lambda series: symbols[Querys.Symbol(series.to_dict())]
                values = stocks[list(Querys.Symbol)].apply(function, axis=1, result_type="expand")
                stocks = pd.concat([stocks, values], axis=1)
            size = self.size(bars)
            self.console(f"{str(symbol)}[{int(size):.0f}]")
            if self.empty(bars): continue
            yield bars

    def download(self, *args, **kwargs):
        bars = self.page(*args, **kwargs)
        assert isinstance(bars, pd.DataFrame)
        return bars

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
    @property
    def api(self): return self.__api


