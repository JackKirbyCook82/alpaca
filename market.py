# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 2025
@name:   Aplaca Market Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from abc import ABC
from datetime import datetime as Datetime
from collections import namedtuple as ntuple

from finance.variables import Variables, Querys, OSI
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging
from support.custom import SliceOrderedDict as SODict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaContractDownloader", "AlpacaStockDownloader", "AlpacaOptionDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


price_parsers = {code: (key, lambda value: np.float32(value)) for key, code in {"last": "p", "ask": "ap", "bid": "bp"}.items()}
size_parsers = {code: (key, lambda value: np.int32(value)) for key, code in {"supply": "as", "demand": "bs"}.items()}
market_parsers = price_parsers | size_parsers

market_parser = lambda mapping: {key: function(mapping[code]) for code, (key, function) in market_parsers.items() if code in mapping.keys()}
stock_parser = lambda mapping: [{"ticker": ticker} | market_parser(content) for ticker, content in mapping.items()]
option_parser = lambda mapping: [dict(OSI(osi)) | market_parser(content) for osi, content in mapping.items()]
expire_parser = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
strike_parser = lambda content: np.round(float(content), 2)


class AlpacaMarketURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}

class AlpacaStockURL(AlpacaMarketURL, domain="https://data.alpaca.markets", path=["v2", "stocks"], parameters={"feed": "delayed_sip"}):
    @staticmethod
    def parameters(*args, tickers, **kwargs):
        assert isinstance(tickers, list)
        return {"symbols": ",".join(list(map(str, tickers)))}

class AlpacaOptionURL(AlpacaMarketURL, domain="https://data.alpaca.markets", path=["v1beta1", "options"], parameters={"feed": "indicative"}):
    @staticmethod
    def parameters(*args, osis, **kwargs):
        assert isinstance(osis, list)
        return {"symbols": ",".join(list(map(str, osis)))}

class AlpacaStockTradeURL(AlpacaStockURL, path=["trades", "latest"]): pass
class AlpacaStockQuoteURL(AlpacaStockURL, path=["quotes", "latest"]): pass
class AlpacaOptionTradeURL(AlpacaOptionURL, path=["trades", "latest"]): pass
class AlpacaOptionQuoteURL(AlpacaOptionURL, path=["quotes", "latest"]): pass

class AlpacaContractURL(AlpacaMarketURL, domain="https://paper-api.alpaca.markets", path=["v2", "options", "contracts"], parameters={"show_deliverables": "false", "limit": "10000"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return tickers | expires | pagination

    @staticmethod
    def tickers(*args, ticker, **kwargs): return {"underlying_symbols": str(ticker)}
    @staticmethod
    def expires(*args, expiry=None, **kwargs): return {"expiration_date_gte": str(expiry.minimum.strftime("%Y-%m-%d")), "expiration_date_lte": str(expiry.maximum.strftime("%Y-%m-%d"))} if bool(expiry) else {}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if bool(pagination) else {}


class AlpacaMarketData(WebJSON.Mapping, multiple=False, optional=False):
    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, list)
        dataframe = pd.DataFrame.from_records(contents)
        return dataframe

class AlpacaStockData(AlpacaMarketData):
    def execute(self, *args, **kwargs):
        dataframe = super().execute(*args, **kwargs)
        dataframe["instrument"] = Variables.Securities.Instrument.STOCK
        dataframe["option"] = Variables.Securities.Option.EMPTY
        return dataframe

class AlpacaOptionData(AlpacaMarketData):
    def execute(self, *args, **kwargs):
        dataframe = super().execute(*args, **kwargs)
        dataframe["instrument"] = Variables.Securities.Instrument.OPTION
        return dataframe

class AlpacaStockTradeData(AlpacaStockData, key="trade", locator="//trades", parser=stock_parser): pass
class AlpacaStockQuoteData(AlpacaStockData, key="quote", locator="//quotes", parser=stock_parser): pass
class AlpacaOptionTradeData(AlpacaOptionData, key="trade", locator="//trades", parser=option_parser): pass
class AlpacaOptionQuoteData(AlpacaOptionData, key="quote", locator="//quotes", parser=option_parser): pass

class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="next_page_token", parser=str, multiple=False, optional=True): pass
    class Contracts(WebJSON, key="contracts", locator="option_contracts", parser=Querys.Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="underlying_symbol", parser=str): pass
        class Expire(WebJSON.Text, key="expire", locator="expiration_date", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="type", parser=Variables.Securities.Option): pass
        class Strike(WebJSON.Text, key="strike", locator="strike_price", parser=strike_parser): pass


class AlpacaMarketPage(WebJSONPage):
    def __init_subclass__(cls, *args, url, data, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.__data__ = data
        cls.__url__ = url

    def execute(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        self.load(url, *args, **kwargs)
        datas = self.data(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents

    @property
    def data(self): return type(self).__data__
    @property
    def url(self): return type(self).__url__

class AlpacaStockTradePage(AlpacaMarketPage, url=AlpacaStockTradeURL, data=AlpacaStockTradeData): pass
class AlpacaStockQuotePage(AlpacaMarketPage, url=AlpacaStockQuoteURL, data=AlpacaStockQuoteData): pass
class AlpacaOptionTradePage(AlpacaMarketPage, url=AlpacaOptionTradeURL, data=AlpacaOptionTradeData): pass
class AlpacaOptionQuotePage(AlpacaMarketPage, url=AlpacaOptionQuoteURL, data=AlpacaOptionQuoteData): pass

class AlpacaContractPage(WebJSONPage):
    def execute(self, *args, pagination=None, **kwargs):
        url = AlpacaContractURL(*args, pagination=pagination, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaContractData(self.json, *args, **kwargs)
        contents = [data(*args, **kwargs) for data in datas["contracts"]]
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(contents)
        else: return list(contents) + self.execute(args, pagination=pagination, **kwargs)


class AlpacaSecurityDownloader(Sizing, Emptying, Partition, Logging, ABC, title="Downloaded"):
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.__trade__ = kwargs.get("trade", getattr(cls, "__trade__", None))
        cls.__quote__ = kwargs.get("quote", getattr(cls, "__quote__", None))

    def __init__(self, *args, api, **kwargs):
        super().__init__(*args, **kwargs)
        Technicals = ntuple("Pages", "trade, quote")
        trade = self.trade(*args, **kwargs)
        quote = self.quote(*args, **kwargs)
        self.__pages = Technicals(trade, quote)
        self.__api = api

    def download(self, *args, query, **kwargs):
        trade = self.pages.trade(*args, **kwargs)
        quote = self.pages.quote(*args, **kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        if self.empty(trade) or self.empty(quote): return pd.DataFrame()
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        average = lambda cols: np.round((cols["ask"] + cols["bid"]) / 2, 2)
        missing = lambda cols: np.isnan(cols["last"])
        dataframe = quote.merge(trade, how="outer", on=list(query), sort=False, suffixes=("", "_"))[header]
        dataframe["last"] = dataframe.apply(lambda cols: average(cols) if missing(cols) else cols["last"], axis=1)
        return dataframe

    @staticmethod
    def querys(querys, querytype):
        assert isinstance(querys, (list, dict, querytype))
        assert all([isinstance(query, querytype) for query in querys]) if isinstance(querys, (list, dict)) else True
        if isinstance(querys, querytype): querys = [querys]
        elif isinstance(querys, dict): querys = SODict(querys)
        else: querys = list(querys)
        return querys

    @property
    def trade(self): return type(self).__trade__
    @property
    def quote(self): return type(self).__quote__
    @property
    def pages(self): return self.__pages
    @property
    def api(self): return self.__api


class AlpacaStockDownloader(AlpacaSecurityDownloader, trade=AlpacaStockTradePage, quote=AlpacaStockQuotePage):
    def execute(self, symbols, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        symbols = [symbols[index:index+100] for index in range(0, len(symbols), 100)]
        for symbols in iter(symbols):
            parameters = dict(tickers=[str(symbol.ticker) for symbol in symbols], query=Querys.Symbol, api=self.api)
            stocks = self.download(*args, **parameters, **kwargs)
            assert isinstance(stocks, pd.DataFrame)
            if self.empty(stocks): continue
            if isinstance(symbols, dict):
                function = lambda series: symbols[Querys.Symbol(series.to_dict())]
                values = stocks[list(Querys.Symbol)].apply(function, axis=1, result_type="expand")
                stocks = pd.concat([stocks, values], axis=1)
            symbols = self.keys(stocks, by=Querys.Symbol)
            symbols = ",".join(list(map(str, symbols)))
            size = self.size(stocks)
            self.console(f"{str(symbols)}[{int(size):.0f}]")
            if self.empty(stocks): continue
            yield stocks


class AlpacaOptionDownloader(AlpacaSecurityDownloader, trade=AlpacaOptionTradePage, quote=AlpacaOptionQuotePage):
    def execute(self, contracts, *args, **kwargs):
        contracts = self.querys(contracts, Querys.Contract)
        if not bool(contracts): return
        contracts = [contracts[index:index+100] for index in range(0, len(contracts), 100)]
        for contracts in iter(contracts):
            parameters = dict(osis=list(map(OSI, contracts)), query=Querys.Contract, api=self.api)
            options = self.download(*args, **parameters, **kwargs)
            assert isinstance(options, pd.DataFrame)
            if self.empty(options): continue
            if isinstance(contracts, dict):
                function = lambda series: contracts[Querys.Contract(series.to_dict())]
                values = options[list(Querys.Contract)].apply(function, axis=1, result_type="expand")
                options = pd.concat([options, values], axis=1)
            settlements = self.keys(options, by=Querys.Settlement)
            settlements = ",".join(list(map(str, settlements)))
            size = self.size(options)
            self.console(f"{str(settlements)}[{int(size):.0f}]")
            if self.empty(options): continue
            yield options


class AlpacaContractDownloader(Logging, title="Downloaded"):
    def __init__(self, *args, api, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaContractPage(*args, **kwargs)
        self.__api = api

    def execute(self, symbols, *args, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol in iter(symbols):
            parameters = dict(ticker=str(symbol.ticker), api=self.api)
            contracts = self.download(*args, **parameters, **kwargs)
            self.console(f"{str(symbol)}[{len(contracts):.0f}]")
            if not bool(contracts): continue
            yield contracts

    def download(self, *args, **kwargs):
        contracts = self.page(*args, **kwargs)
        assert isinstance(contracts, list)
        contracts.sort(key=lambda contract: contract.expire)
        return contracts

    @staticmethod
    def querys(querys, querytype):
        assert isinstance(querys, (list, querytype))
        assert all([isinstance(query, querytype) for query in querys]) if isinstance(querys, list) else True
        querys = list(querys) if isinstance(querys, list) else [querys]
        return querys

    @property
    def page(self): return self.__page
    @property
    def api(self): return self.__api




