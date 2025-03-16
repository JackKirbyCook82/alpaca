# -*- coding: utf-8 -*-
"""
Created on Sat Mar 15 2025
@name:   Aplaca Portfolio Objects
@author: Jack Kirby Cook

"""

from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaPortfolioDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaPortfolioURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "positions"], headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaPortfolioData(WebJSON, key="portfolio", multiple=True, optional=True):
    pass


class AlpacaPortfolioPage(WebJSONPage):
    def __init_subclass__(cls, *args, **kwargs):
        cls.__data__ = AlpacaPortfolioData
        cls.__url__ = AlpacaPortfolioURL

    def execute(self, *args, **kwargs):
        pass

    @property
    def data(self): return type(self).__data__
    @property
    def url(self): return type(self).__url__


class AlpacaPortfolioDownloader(Sizing, Emptying, Partition, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaPortfolioPage(*args, **kwargs)

    def execute(self, *args, **kwargs):
        pass

    @property
    def page(self): return self.__page



