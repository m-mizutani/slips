import sys

sys.path.insert(0, './slips/')

import parser

class Queue(parser.Parser):
    def __init__(self):
        self._q = []

    def recv(self, meta: parser.MetaData, data: dict):
        self._q.append((meta, data))

    def fetch(self):
        return self._q

    
def exec_test(builder, events):
    obj = builder()
    q = Queue()
    obj.pipe(q)
    for ev in events:
        meta = parser.MetaData()
        obj.recv(meta, ev)

    return q.fetch()
        
