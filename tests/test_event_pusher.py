import json
import sys
sys.path.append('./slips/')

import event_pusher


def test_event_pusher():
    assert event_pusher.main is not None
    
