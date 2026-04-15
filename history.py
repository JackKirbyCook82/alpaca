# -*- coding: utf-8 -*-
"""
Created on Thurs Mar 26 2026
@name:   Alpaca History Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from abc import ABC, abstractmethod
from datetime import timezone as Timezone
from datetime import datetime as Datetime

from webscraping.webpages import WebJSONPage, WebStream
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.finance import Concepts, Alerting

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaBarsDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


pagination_parser = lambda string: str(string) if string != "None" else None
history_parser = lambda string: Datetime.strptime(string, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=Timezone.utc).date()


class AlpacaHistoryURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaBarsURL(AlpacaHistoryURL, domain="https://data.alpaca.markets", path=["v2", "stocks", "bars"], parameters={"timeframe": "1Day", "feed": "sip", "limit": "10000"}):
    @staticmethod
    def parameters(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}

    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        history = cls.history(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return tickers | history | pagination

    @staticmethod
    def tickers(*args, tickers, **kwargs): return {"symbols": ",".join(list(tickers))}
    @staticmethod
    def history(*args, history, **kwargs): return {"start": history.minimum.strftime("%Y-%m-%d"), "end": history.maximum.strftime("%Y-%m-%d")}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs):
        if pagination is not None: return {"page_token": str(pagination)}
        else: return {}


class AlpacaHistoryData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, multiple=False, optional=True): pass


@dataclass(frozen=True)
class AlpacaField: name: str; code: str; parser: callable


class AlpacaHistoryPage(WebJSONPage, ABC): pass
class AlpacaBarsPage(AlpacaHistoryPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        fields = [AlpacaField("open", "o", np.float32), AlpacaField("close", "c", np.float32), AlpacaField("high", "h", np.float32), AlpacaField("low", "l", np.float32), AlpacaField("adjusted", "vw", np.float32)]
        fields = fields + [AlpacaField("date", "t", history_parser), AlpacaField("volume", "v", np.int64)]
        self.__fields = fields

    def __call__(self, *args, tickers, history, **kwargs):
        parameters = dict(tickers=tickers, history=history, authenticator=self.authenticator)
        records = self.bars(**parameters)
        bars = pd.DataFrame.from_records(records)
        return bars

    def bars(self, *args, pagination=None, **kwargs):
        url = AlpacaBarsURL(*args, pagination=pagination, **kwargs)
        json = self.load(url)
        records = [{"ticker": ticker} | self.parser(mapping) for ticker, contents in json["bars"].items() for mapping in contents]
        datas = AlpacaHistoryData(json, *args, **kwargs)
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.bars(*args, pagination=pagination, **kwargs)

    def parser(self, mapping):
        return {field.name: field.parser(mapping[field.code]) for field in self.fields if field.code in mapping.keys()}

    @property
    def fields(self): return self.__fields


class AlpacaHistoryDownloader(WebStream, Alerting, ABC):
    @abstractmethod
    def downloader(self, *args, **kwargs): pass


class AlpacaBarsDownloader(AlpacaHistoryDownloader, page=AlpacaBarsPage):
    def __call__(self, symbols, *args, **kwargs):
        assert isinstance(symbols, list)
        tickers = list({symbol.ticker for symbol in symbols})
        bars = self.downloader(tickers, *args, **kwargs)
        bars = pd.concat(list(bars), axis=0)
        bars["date"] = pd.to_datetime(bars["date"])
        bars = bars.sort_values(by=["ticker", "date"], ascending=[True, False], inplace=False)
        return bars

    def downloader(self, tickers, *args, **kwargs):
        tickers = [tickers[index:index+self.capacity] for index in range(0, len(tickers), self.capacity)]
        for tickers in tickers:
            bars = self.page(*args, tickers=tickers, **kwargs)
            if bool(bars.empty): continue
            self.alert(bars, title="Downloaded", instrument=Concepts.Securities.Instrument.STOCK)
            yield bars




