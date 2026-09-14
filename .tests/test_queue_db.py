from uuid import UUID

from wa_agents import queue_db


class _StorageStub :

    def __init__(self) -> None :
        self.released = []

    def get_inbound_message( self, msg_id : str) -> dict :
        return {
            "id"       : 21,
            "msg_id"   : msg_id,
            "contact"  : 31,
            "business" : 41,
        }

    def release_contact_lease( self, contact, owner_token) -> bool :
        self.released.append(( contact, owner_token))
        return True


def test_queue_enqueues_persisted_message_id( monkeypatch) -> None :
    queue = queue_db.QueueDB("postgresql://test")
    calls = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return { "id" : 1 }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)

    assert queue.enqueue("wamid.ABC123=") is True
    assert calls == [
        ( queue_db.SQL_ENQUEUE, { "msg_id" : "wamid.ABC123=" } ),
    ]


def test_queue_claim_returns_message_and_lease_identity( monkeypatch) -> None :
    queue         = queue_db.QueueDB("postgresql://test")
    queue.storage = _StorageStub()
    calls         = []

    def fake_fetch_one( sql, params) :
        calls.append(( sql, params))
        return {
            "row_id"     : 1,
            "msg_id"     : "wamid.ABC123=",
            "msg_status" : "processing",
            "contact"    : 31,
        }

    monkeypatch.setattr( queue, "_fetch_one", fake_fetch_one)
    item = queue.claim_next()

    assert item["id"] == 21
    assert item["row_id"] == 1
    assert item["contact"] == 31
    assert isinstance( item["owner_token"], UUID)
    assert calls[0][0] == queue_db.SQL_CLAIM_NEXT
    assert calls[0][1]["owner_token"] == item["owner_token"]
