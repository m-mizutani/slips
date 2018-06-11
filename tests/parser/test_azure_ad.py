import json
import sys

import helper

sys.path.insert(0, './slips/')

import parser


def test_audit_event():
    data = json.load(open('./tests/parser/data/azure_ad/audit.json'))
    qdata = helper.exec_test(parser.AzureAdAudit, [data])

    assert len(qdata) == 1
    m, d = qdata[0]
    assert m.tag == 'azure_ad.audit'
    assert int(m.timestamp) == 1528658867


def test_risk_event():
    data = json.load(open('./tests/parser/data/azure_ad/risk_event.json'))
    qdata = helper.exec_test(parser.AzureAdRiskEvent, [data])

    assert len(qdata) == 1
    m, d = qdata[0]
    assert m.tag == 'azure_ad.risk_event'
    assert int(m.timestamp) == 1521147226

    
