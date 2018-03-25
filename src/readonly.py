import slips.interface
import logging
import collections


class MyTest(slips.interface.Handler):
    def __init__(self):
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)
        self._results = collections.defaultdict(int)
        
    def setup(self, args):
        self._logger.info('ARGS > %s', args)

    def recv(self, meta, event):
        self._results[meta.tag] += 1

    def result(self):
        return dict(self._results)
