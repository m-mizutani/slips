import json
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


def test_audit_event():
    psr = parser.AzureAdAudit()
    q = Queue()
    psr.pipe(q)

    meta = parser.MetaData()
    meta.tag = 'xxx'
    data = json.load(open('./tests/parser/data/azure_ad/audit.json'))

    psr.recv(meta, data)
    qdata = q.fetch()

    assert len(qdata) == 1
    m, d = qdata[0]
    assert int(m.timestamp) == 1528658867
