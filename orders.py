# -*- coding: utf-8 -*-
"""
Created on Sat May 16 2026
@name:   Alpaca Order Objects
@author: Jack Kirby Cook

"""

from abc import ABC, abstractmethod

from webscraping.webpages import WebStream, WebJSONPage
from webscraping.weburl import WebURL
from support.finance import Alerting

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaSpreadUploader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaOrderURL(WebURL, headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaSpreadURL(AlpacaOrderURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"]):
    pass


class AlpacaSpreadPayload(object):
    pass


class AlpacaOrderPage(WebJSONPage, ABC): pass
class AlpacaSpreadPage(AlpacaOrderPage):
    def __call__(self, *args, spread, **kwargs):
        pass


class AlpacaOrderUploader(WebStream, Alerting, ABC):
    @abstractmethod
    def uploader(self, *args, **kwargs): pass


class AlpacaSpreadUploader(AlpacaOrderUploader, page=AlpacaSpreadPage):
    def __call__(self, spreads, *args, **kwargs):
        pass

    def uploader(self, spreads, *args, **kwargs):
        pass


