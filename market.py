# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 2025
@name:   Aplaca Market Objects
@author: Jack Kirby Cook

"""

import pytz
import numpy as np
import pandas as pd
from itertools import groupby
from datetime import datetime as Datetime
from collections import namedtuple as ntuple

from finance.variables import Variables, Querys, OSI
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaContractDownloader", "AlpacaStockDownloader", "AlpacaOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


timestamp_parser = lambda string: Datetime.fromisoformat(string).astimezone(pytz.timezone("US/Central"))
current_parser = lambda string: np.datetime64(timestamp_parser(string))
price_parsers = {code: (key, lambda value: np.float32(value)) for key, code in {"price": "p", "ask": "ap", "bid": "bp"}.items()}
size_parsers = {code: (key, lambda value: np.int32(value)) for key, code in {"supply": "as", "demand": "bs"}.items()}
date_parsers = {"t": ("current", lambda value: current_parser(value))}
contents_parsers = price_parsers | size_parsers | date_parsers
contents_parser = lambda contents: {key: function(contents[code]) for code, (key, function) in contents_parsers.items() if code in contents.keys()}
stock_parser = lambda mapping: [{"ticker": ticker} | contents_parser(contents) for ticker, contents in mapping.items()]
option_parser = lambda mapping: [dict(OSI(osi)) | contents_parser(contents) for osi, contents in mapping.items()]
strike_parser = lambda content: np.round(float(content), 2).astype(np.float32)
expire_parser = lambda string: Datetime.strptime(string, "%Y-%m-%d")


class AlpacaURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}

class AlpacaStockURL(AlpacaURL, domain="https://data.alpaca.markets", path=["v2", "stocks"], parameters={"feed": "delayed_sip"}):
    @staticmethod
    def parameters(*args, tickers, **kwargs):
        assert isinstance(tickers, list)
        return {"symbols": ",".join(list(map(str, tickers)))}

class AlpacaOptionURL(AlpacaURL, domain="https://data.alpaca.markets", path=["v1beta1", "options"], parameters={"feed": "indicative"}):
    @staticmethod
    def parameters(*args, osis, **kwargs):
        assert isinstance(osis, list)
        return {"symbols": ",".join(list(map(str, osis)))}

class AlpacaStockTradeURL(AlpacaStockURL, path=["trades", "latest"]): pass
class AlpacaStockQuoteURL(AlpacaStockURL, path=["quotes", "latest"]): pass
class AlpacaOptionTradeURL(AlpacaOptionURL, path=["trades", "latest"]): pass
class AlpacaOptionQuoteURL(AlpacaOptionURL, path=["quotes", "latest"]): pass

class AlpacaContractURL(AlpacaURL, domain="https://paper-api.alpaca.markets", path=["v2", "options", "contracts"], parameters={"show_deliverables": "false", "limit": "10000"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return tickers | expires | pagination

    @staticmethod
    def tickers(*args, ticker, **kwargs): return {"underlying_symbols": str(ticker)}
    @staticmethod
    def expires(*args, expires=None, **kwargs): return {"expiration_date_gte": str(expires.minimum.strftime("%Y-%m-%d")), "expiration_date_lte": str(expires.maximum.strftime("%Y-%m-%d"))} if bool(expires) else {}
    @staticmethod
    def strikes(*args, strikes=None, **kwargs): return {"strike_price_gte": min(strikes), "strike_price_lte": max(strikes)} if bool(strikes) else {}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if bool(pagination) else {}


class AlpacaData(WebJSON.Mapping, multiple=False, optional=False):
    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, list)
        return pd.DataFrame.from_records(contents)

class AlpacaStockTradeData(AlpacaData, locator="//trades", parser=stock_parser): pass
class AlpacaStockQuoteData(AlpacaData, locator="//quotes", parser=stock_parser): pass
class AlpacaOptionTradeData(AlpacaData, locator="//trades", parser=option_parser): pass
class AlpacaOptionQuoteData(AlpacaData, locator="//quotes", parser=option_parser): pass

class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, locator="next_page_token", key="pagination", parser=str, multiple=False, optional=True): pass
    class Contracts(WebJSON, locator="option_contracts", key="contracts", parser=Querys.Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, locator="underlying_symbol", key="ticker", parser=str): pass
        class Expire(WebJSON.Text, locator="expiration_date", key="expire", parser=expire_parser): pass
        class Option(WebJSON.Text, locator="type", key="option", parser=Variables.Securities.Option): pass
        class Strike(WebJSON.Text, locator="strike_price", key="strike", parser=strike_parser): pass


class AlpacaPage(WebJSONPage):
    def __init_subclass__(cls, *args, url, data, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.__data__ = data
        cls.__url__ = url

    def execute(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        self.load(url, *args, **kwargs)
        data = self.data(self.json, *args, **kwargs)
        contents = data(*args, **kwargs)
        return contents

    @property
    def data(self): return type(self).__data__
    @property
    def url(self): return type(self).__url__

class AlpacaStockTradePage(AlpacaPage, url=AlpacaStockTradeURL, data=AlpacaStockTradeData): pass
class AlpacaStockQuotePage(AlpacaPage, url=AlpacaStockQuoteURL, data=AlpacaStockQuoteData): pass
class AlpacaOptionTradePage(AlpacaPage, url=AlpacaOptionTradeURL, data=AlpacaOptionTradeData): pass
class AlpacaOptionQuotePage(AlpacaPage, url=AlpacaOptionQuoteURL, data=AlpacaOptionQuoteData): pass

class AlpacaContractPage(WebJSONPage):
    def execute(self, *args, pagination=None, **kwargs):
        url = AlpacaContractURL(*args, pagination=pagination, **kwargs)
        self.load(url, *args, **kwargs)
        data = AlpacaContractData(self.json, *args, **kwargs)
        contents = [contract(*args, **kwargs) for contract in data["contracts"]]
        pagination = data["pagination"](*args, **kwargs)
        if not bool(pagination): return list(contents)
        else: return list(contents) + self.execute(args, pagination=pagination, **kwargs)


class AlpacaStockDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pages = ntuple("Pages", "trade quote")
        trade = AlpacaStockTradePage(*args, **kwargs)
        quote = AlpacaStockQuotePage(*args, **kwargs)
        self.__pages = pages(trade, quote)

    def execute(self, symbols, *args, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        if not bool(symbols): return
        parameters = dict(tickers=[str(symbol.ticker) for symbol in symbols])
        dataframe = self.download(*args, **parameters, **kwargs)
        assert isinstance(dataframe, pd.DataFrame)
        for symbol, stocks in self.partition(dataframe, by=Querys.Symbol):
            size = self.size(stocks)
            self.console(f"{str(symbol)}[{int(size):.0f}]")
            if self.empty(stocks): return
            yield stocks

    def download(self, *args, **kwargs):
        trade = self.pages.trade(*args, **kwargs)
        quote = self.pages.quote(*args, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        average = lambda cols: np.round((cols["ask"] + cols["bid"]) / 2, 2).astype(np.float32)
        missing = lambda cols: np.isnan(cols["price"])
        dataframe = quote.merge(trade, how="outer", on=list(Querys.Symbol), sort=False, suffixes=("", "_"))[header]
        dataframe["price"] = dataframe.apply(lambda cols: average(cols) if missing(cols) else cols["price"], axis=1)
        return dataframe

    @property
    def pages(self): return self.__pages


class AlpacaOptionDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pages = ntuple("Pages", "trade quote")
        trade = AlpacaOptionTradePage(*args, **kwargs)
        quote = AlpacaOptionQuotePage(*args, **kwargs)
        self.__pages = pages(trade, quote)

    def execute(self, contracts, *args, **kwargs):
        assert isinstance(contracts, (list, Querys.Contract))
        assert all([isinstance(contract, Querys.Contract) for contract in contracts]) if isinstance(contracts, list) else True
        contracts = list(contracts) if isinstance(contracts, list) else [contracts]
        if not bool(contracts): return
        contracts = [contracts[index:index+100] for index in range(0, len(contracts), 100)]
        for contracts in iter(contracts):
            parameters = dict(osis=[OSI([contract.ticker, contract.expire, contract.option, contract.strike]) for contract in contracts])
            dataframe = self.download(*args, **parameters, **kwargs)
            assert isinstance(dataframe, pd.DataFrame)
            for settlement, options in self.partition(dataframe, by=Querys.Settlement):
                size = self.size(options)
                self.console(f"{str(settlement)}[{int(size):.0f}]")
                if self.empty(options): continue
                yield options

    def download(self, *args, **kwargs):
        trade = self.pages.trade(*args, **kwargs)
        quote = self.pages.quote(*args, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        average = lambda cols: np.round((cols["ask"] + cols["bid"]) / 2, 2).astype(np.float32)
        missing = lambda cols: np.isnan(cols["price"])
        dataframe = quote.merge(trade, how="outer", on=list(Querys.Contract), sort=False, suffixes=("", "_"))[header]
        dataframe["price"] = dataframe.apply(lambda cols: average(cols) if missing(cols) else cols["price"], axis=1)
        return dataframe

    @property
    def pages(self): return self.__pages


class AlpacaContractDownloader(Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaContractPage(*args, **kwargs)

    def execute(self, symbols, *args, expires, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        if not bool(symbols): return
        for symbol in iter(symbols):
            parameters = dict(ticker=str(symbol.ticker), expires=expires)
            contracts = self.download(*args, **parameters, **kwargs)
            self.console(f"{str(symbol)}[{len(contracts):.0f}]")
            if not bool(contracts): continue
            yield contracts

    def download(self, *args, **kwargs):
        contracts = self.page(*args, **kwargs)
        assert isinstance(contracts, list)
        contracts.sort(key=lambda contract: contract.expire)
        return contracts

    @property
    def page(self): return self.__page




