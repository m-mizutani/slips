# -*- coding: utf-8 -*-

import abc


class Handler(abc.ABC):
    @abc.abstractmethod
    def setup(self, args):
        pass
    
    @abc.abstractmethod
    def recv(self, meta, event):
        pass

    @abc.abstractmethod
    def result(self):
        pass
    
