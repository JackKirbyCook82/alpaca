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

from finance.concepts import Concepts, Querys, OSI
from webscraping.websupport import WebDownloader
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


Field = ntuple("Field", "name code parser")
market_fields = [Field("last", "p", np.float32), Field("bid", "dp", np.float32), Field("ask", "ap", np.float32), Field("supply", "as", np.float32), Field("demand", "bs", np.float32)]
market_parser = lambda mapping: {field.name: field.parser(mapping[field.code]) for field in market_fields}
stocks_parser = lambda contents: [{"ticker": ticker} | market_parser(mapping) for ticker, mapping in contents.items()]
options_parser = lambda contents: [dict(OSI.parse(osi)) | market_parser(content) for osi, mapping in contents.items()]
expire_parser = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
strike_parser = lambda content: np.round(float(content), 2)


class AlpacaMarketURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs): return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}

class AlpacaSecurityURL(AlpacaMarketURL):
    @staticmethod
    def parameters(*args, products, **kwargs): return {"symbols": ",".join(list(map(str, products)))}

class AlpacaStockURL(AlpacaSecurityURL, domain="https://data.alpaca.markets", path=["v2", "stocks"], parameters={"feed": "delayed_sip"}): pass
class AlpacaOptionURL(AlpacaSecurityURL, domain="https://data.alpaca.markets", path=["v1beta1", "options"], parameters={"feed": "indicative"}): pass

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
        assert isinstance(contents, list) # <stocks_parser> & <options_parser> return []
        dataframe = pd.DataFrame.from_records(contents)
        return dataframe

class AlpacaStockData(AlpacaMarketData):
    def execute(self, *args, **kwargs):
        dataframe = super().execute(*args, **kwargs)
        dataframe["instrument"] = Concepts.Securities.Instrument.STOCK
        dataframe["option"] = Concepts.Securities.Option.EMPTY
        return dataframe

class AlpacaOptionData(AlpacaMarketData):
    def execute(self, *args, **kwargs):
        dataframe = super().execute(*args, **kwargs)
        dataframe["instrument"] = Concepts.Securities.Instrument.OPTION
        return dataframe

class AlpacaStockTradeData(AlpacaStockData, key="trade", locator="//trades", parser=stocks_parser): pass
class AlpacaStockQuoteData(AlpacaStockData, key="quote", locator="//quotes", parser=stocks_parser): pass
class AlpacaOptionTradeData(AlpacaOptionData, key="trade", locator="//trades", parser=options_parser): pass
class AlpacaOptionQuoteData(AlpacaOptionData, key="quote", locator="//quotes", parser=options_parser): pass

class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=str, multiple=False, optional=True): pass
    class Contracts(WebJSON, key="contracts", locator="//option_contracts[]", parser=Querys.Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="//underlying_symbol", parser=str): pass
        class Expire(WebJSON.Text, key="expire", locator="//expiration_date", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="//type", parser=Concepts.Securities.Option): pass
        class Strike(WebJSON.Text, key="strike", locator="//strike_price", parser=strike_parser): pass


class AlpacaStockTradePage(WebJSONPage):
    def execute(self, *args, symbols, **kwargs):
        symbols = [str(symbol) for symbol in symbols]
        parameters = dict(products=symbols, authenticator=self.source.authenticator)
        url = AlpacaStockTradeURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaStockTradeData(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents

class AlpacaStockQuotePage(WebJSONPage):
    def execute(self, *args, symbols, **kwargs):
        symbols = [str(symbol) for symbol in symbols]
        parameters = dict(products=symbols, authenticator=self.source.authenticator)
        url = AlpacaStockQuoteURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaStockQuoteData(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents

class AlpacaOptionTradePage(WebJSONPage):
    def execute(self, *args, contracts, **kwargs):
        contracts = [str(contract) for contract in contracts]
        parameters = dict(products=contracts, authenticator=self.source.authenticator)
        url = AlpacaOptionTradeURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaOptionTradeData(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents

class AlpacaOptionQuotePage(WebJSONPage):
    def execute(self, *args, contracts, **kwargs):
        contracts = [str(contract) for contract in contracts]
        parameters = dict(products=contracts, authenticator=self.source.authenticator)
        url = AlpacaOptionQuoteURL(*args, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaOptionQuoteData(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents

class AlpacaContractPage(AlpacaMarketPage):
    def execute(self, *args, symbol, expiry, pagination=None, **kwargs):
        parameters = dict(ticker=str(symbol.ticker), expiry=expiry, authenticator=self.source.authenticator)
        url = AlpacaContractURL(*args, pagination=pagination, **parameters, **kwargs)
        self.load(url, *args, **kwargs)
        datas = AlpacaContractData(self.json, *args, delayer=self.delayer, **kwargs)
        contents = [data(*args, **kwargs) for data in datas["contracts"]]
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(contents)
        else: return list(contents) + self.execute(args, pagination=pagination, **parameters, **kwargs)


class AlpacaSecurityDownloader(WebDownloader, ABC):
    def download(self, /, query, **kwargs):
        trade = self.pages.trade(**kwargs)
        quote = self.pages.quote(**kwargs)
        assert isinstance(trade, pd.DataFrame) and isinstance(quote, pd.DataFrame)
        if self.empty(trade) or self.empty(quote): return pd.DataFrame()
        header = list(trade.columns) + [column for column in list(quote.columns) if column not in list(trade.columns)]
        average = lambda cols: np.round((cols["ask"] + cols["bid"]) / 2, 2)
        missing = lambda cols: np.isnan(cols["last"])
        dataframe = quote.merge(trade, how="outer", on=list(query), sort=False, suffixes=("", "_"))[header]
        dataframe["last"] = dataframe.apply(lambda cols: average(cols) if missing(cols) else cols["last"], axis=1)
        return dataframe


class AlpacaStockDownloader(AlpacaSecurityDownloader, pages={"trade": AlpacaStockTradePage, "quote": AlpacaStockQuotePage}):
    def execute(self, symbols, /, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        symbols = [symbols[index:index+100] for index in range(0, len(symbols), 100)]
        for symbols in iter(symbols):
            stocks = self.download(symbols=symbols, query=Querys.Symbol, **kwargs)
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


class AlpacaOptionDownloader(AlpacaSecurityDownloader, pages={"trade": AlpacaOptionTradePage, "quote": AlpacaOptionQuotePage}):
    def execute(self, contracts, /, **kwargs):
        contracts = self.querys(contracts, Querys.Contract)
        if not bool(contracts): return
        contracts = [contracts[index:index+100] for index in range(0, len(contracts), 100)]
        for contracts in iter(contracts):
            options = self.download(contracts=contracts, query=Querys.Contract, **kwargs)
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


class AlpacaContractDownloader(WebDownloader, page=AlpacaContractPage):
    def execute(self, symbols, /, expiry=None, **kwargs):
        symbols = self.querys(symbols, Querys.Symbol)
        if not bool(symbols): return
        for symbol in iter(symbols):
            contracts = self.download(symbol=symbol, expiry=expiry, **kwargs)
            self.console(f"{str(symbol)}[{len(contracts):.0f}]")
            if not bool(contracts): continue
            yield contracts

    def download(self, /, **kwargs):
        contracts = self.page(**kwargs)
        assert isinstance(contracts, list)
        contracts.sort(key=lambda contract: contract.expire)
        return contracts





