import logging
import slips.interface


class YourHandler(slips.interface.Handler):
    def __init__(self):
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)
        
    def setup(self, args):
        self._logger.info('ARGS > %s', args)
        
    def recv(self, meta, event):
        self._logger.info('log meta data: %s', meta)
        self._logger.info('log data: %s', event)

    def result(self):
        return 'ok'  # Return some value if you need.
