from datetime import (
    UTC,
    datetime,
)

import pytest

from wa_agents import case_handler_base
from wa_agents.case_handler_base import (
    AsyncCaseHandlerBase,
    AsyncWhatsAppCaseHandler,
    CH_State,
    CaseHandlerBase,
    WhatsAppCaseHandler,
)
from wa_agents.case_handler_models import (
    CaseManifest,
    Message,
    ServerTextMsg,
)
from wa_agents.supabase import (
    SyncSupabaseStorage,
    WhatsAppDatabaseRecord_Business,
    WhatsAppDatabaseRecord_Contact,
)
from wa_agents.whatsapp_models import WhatsAppProfile


class _StorageStub :

    def __init__( self, manifest : CaseManifest) -> None :
        self.manifest      = manifest
        self.messages      = []
        self.insert_states = []

    def get_open_case_manifest( self, _contact_id : int) -> CaseManifest :
        return self.manifest

    def get_case_handler_messages( self, _case_id : int) -> list[Message] :
        return self.messages

    def insert_case_handler_message(
        self,
        _case_id       : int,
        message        : Message,
        machine_state  : str | None,
    ) -> Message :
        self.insert_states.append(machine_state)
        return message.model_copy( update = { "id" : 51 })


class _StateHandler (CaseHandlerBase) :

    @classmethod
    def define_state_machine_config(cls) :
        return [ CH_State("start"), CH_State("ready") ], "start", []

    def __init__( self, manifest : CaseManifest) -> None :
        business = WhatsAppDatabaseRecord_Business(
            row_id               = 41,
            waba_id              = "123456789012345",
            phone_number_id      = "1234567890",
            display_phone_number = "15551234567",
        )
        contact = WhatsAppDatabaseRecord_Contact(
            row_id  = 31,
            profile = WhatsAppProfile( name = "Test User"),
            wa_id   = "593995341161",
        )
        super().__init__(
            business,
            contact,
            database_url = "postgresql://test",
        )
        self.ingested = []
        self.storage  = _StorageStub(manifest)
        self.init_machine()

    def ingest_message( self, message : Message) -> None :
        self.ingested.append(message)
        self.state = "ready"

    def process_message( self, _message, _media_content = None) -> bool :
        return False

    def generate_response( self, _max_tokens = None) -> bool :
        return False


class _LookupStorage :

    resolve_business_and_contact = SyncSupabaseStorage.resolve_business_and_contact

    def __init__( self, database_url : str | None = None) -> None :
        self.database_url = database_url or "postgresql://test"

    def get_business( self, business : int) -> dict | None :
        if business != 41 :
            return None
        return {
            "id"                   : 41,
            "waba_id"              : "123456789012345",
            "phone_number_id"      : "1234567890",
            "display_phone_number" : "15551234567",
        }

    def get_contact( self, contact : int) -> dict | None :
        if contact != 31 :
            return None
        return {
            "id"               : 31,
            "business"         : 41,
            "wa_id"            : "593995341161",
            "user_id"          : None,
            "profile_name"     : "Test User",
            "profile_username" : None,
        }


class _IDHandler(CaseHandlerBase) :

    def process_message( self, _message) -> bool :
        return False

    def generate_response( self, _max_tokens = None) -> bool :
        return False


class _AsyncIDHandler(AsyncCaseHandlerBase) :

    async def process_message( self, _message) -> bool :
        return False

    async def generate_response( self, _max_tokens = None) -> bool :
        return False


def _manifest( machine_state : str = "ready") -> CaseManifest :
    return CaseManifest(
        id            = 11,
        contact       = 31,
        created_at    = datetime.now(UTC),
        is_open       = True,
        machine_state = machine_state,
    )


def test_context_build_restores_fsm_without_replay() -> None :
    handler                  = _StateHandler(_manifest())
    handler.storage.messages = [ ServerTextMsg( id = 50, text = "old") ]

    handler.context_build()

    assert handler.state == "ready"
    assert handler.ingested == []
    assert len(handler.case_context) == 1


def test_context_update_persists_resulting_fsm_state_atomically() -> None :
    handler               = _StateHandler(_manifest("start"))
    handler.case_id       = handler.storage.manifest.id
    handler.case_manifest = handler.storage.manifest
    handler.case_context  = []

    stored = handler.context_update(ServerTextMsg(text = "new"))

    assert handler.ingested
    assert handler.storage.insert_states == [ "ready" ]
    assert stored.id == 51
    assert handler.case_manifest.machine_state == "ready"
    assert handler.case_manifest.message_ids == [ 51 ]
    assert handler.case_context == [ stored ]


def test_whatsapp_methods_live_on_transport_adapters() -> None :
    assert issubclass( WhatsAppCaseHandler, CaseHandlerBase)
    assert issubclass( AsyncWhatsAppCaseHandler, AsyncCaseHandlerBase)
    assert "_get_media_storage" in CaseHandlerBase.__dict__
    assert "_get_media_storage" in AsyncCaseHandlerBase.__dict__
    assert "_get_media_storage" not in WhatsAppCaseHandler.__dict__
    assert "_get_media_storage" not in AsyncWhatsAppCaseHandler.__dict__
    assert "_hydrate_media" in CaseHandlerBase.__dict__
    assert "_hydrate_media" in AsyncCaseHandlerBase.__dict__
    assert not hasattr( CaseHandlerBase, "send_text")
    assert not hasattr( AsyncCaseHandlerBase, "send_text")
    assert hasattr( WhatsAppCaseHandler, "send_text")
    assert hasattr( AsyncWhatsAppCaseHandler, "send_text")


def test_handler_resolves_business_and_contact_ids( monkeypatch) -> None :
    monkeypatch.setattr( case_handler_base, "SyncSupabaseStorage", _LookupStorage)

    handler = _IDHandler( 41, 31, database_url = "postgresql://test")

    assert handler.business_id == 41
    assert handler.contact_id == 31
    assert handler.operator_id == "1234567890"
    assert handler.user_id == "593995341161"
    assert handler.user_name == "Test User"


def test_async_handler_resolves_business_and_contact_ids( monkeypatch) -> None :
    class _AsyncStorage :

        def __init__( self, database_url : str | None = None) -> None :
            self.database_url = database_url or "postgresql://test"

    monkeypatch.setattr( case_handler_base, "SyncSupabaseStorage", _LookupStorage)
    monkeypatch.setattr( case_handler_base, "AsyncSupabaseStorage", _AsyncStorage)

    handler = _AsyncIDHandler( 41, 31, database_url = "postgresql://test")

    assert handler.business_id == 41
    assert handler.contact_id == 31
    assert handler.operator_id == "1234567890"
    assert handler.user_id == "593995341161"


def test_handler_rejects_contact_from_another_business( monkeypatch) -> None :
    class _WrongBusinessStorage(_LookupStorage) :

        def get_contact( self, contact : int) -> dict | None :
            row = super().get_contact(contact)
            if row :
                row["business"] = 99
            return row

    monkeypatch.setattr(
        case_handler_base,
        "SyncSupabaseStorage",
        _WrongBusinessStorage,
    )

    with pytest.raises( ValueError, match = "does not belong") :
        _IDHandler( 41, 31, database_url = "postgresql://test")
