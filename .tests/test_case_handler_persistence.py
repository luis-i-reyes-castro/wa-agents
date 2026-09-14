from datetime import (
    UTC,
    datetime,
)

from wa_agents.case_handler_base import (
    CH_State,
    CaseHandlerBase,
)
from wa_agents.case_handler_models import (
    CaseManifest,
    Message,
    ServerTextMsg,
)
from wa_agents.whatsapp_models import (
    WhatsAppContact,
    WhatsAppMetaData,
    WhatsAppProfile,
)


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
        operator = WhatsAppMetaData(
            display_phone_number = "15551234567",
            phone_number_id      = "1234567890",
        )
        user = WhatsAppContact(
            profile = WhatsAppProfile( name = "Test User"),
            wa_id   = "593995341161",
        )
        super().__init__(
            operator,
            user,
            business_id  = 41,
            contact_id   = 31,
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
