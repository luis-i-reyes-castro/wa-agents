import asyncio

from datetime import (
    UTC,
    datetime,
)

import pytest

from wa_agents import case_handler_base
from wa_agents.case_handler_base import (
    Async_CH_StateMachine,
    AsyncCaseHandlerBase,
    AsyncWhatsAppCaseHandler,
    CaseHandlerState,
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
from wa_agents.whatsapp_models import (
    WhatsAppMessage,
    WhatsAppProfile,
)


class _StorageStub :

    def __init__( self, manifest : CaseManifest) -> None :
        self.manifest      = manifest
        self.messages      = []
        self.agent_contexts = {}
        self.insert_states = []
        self.inserted_handler_ids = []
        self.inserted_agent_names = []
        self.context_changes = []
        self.inserted_texts  = []

    def get_open_case_manifest( self, _contact_id : int) -> CaseManifest :
        return self.manifest

    def get_case_handler_messages( self, _case_id : int) -> list[Message] :
        return self.messages

    def get_agent_contexts( self, _case_id : int) -> dict[str, list[int]] :
        return self.agent_contexts

    def insert_case_handler_message(
        self,
        _case_id       : int,
        message        : Message,
        machine_state  : str | None,
        agent_contexts_to_clear  : list[str] | None = None,
        agent_contexts_to_append : list[str] | None = None,
    ) -> Message :
        self.insert_states.append(machine_state)
        self.inserted_texts.append(message.text)
        self.context_changes.append(
            (
                agent_contexts_to_clear or [],
                agent_contexts_to_append or [],
            )
        )
        return message.model_copy( update = { "id" : 51 })

    def insert_case_manifest(
        self,
        handler_id    : int | None,
        contact       : int,
        machine_state : str | None,
        agent_names   : list[str] | None = None,
    ) -> CaseManifest :
        self.inserted_handler_ids.append(handler_id)
        self.inserted_agent_names.append(agent_names or [])
        self.manifest = CaseManifest(
            id            = 12,
            contact       = contact,
            handler_id    = handler_id,
            machine_state = machine_state,
        )
        return self.manifest

    def update_case_manifest( self, manifest : CaseManifest) -> CaseManifest :
        self.manifest = manifest
        return manifest


class _StateHandler (CaseHandlerBase) :

    @classmethod
    def define_state_machine_config(cls) :
        return [ CaseHandlerState("start"), CaseHandlerState("ready") ], "start", []

    def __init__(
        self,
        manifest   : CaseManifest,
        handler_id : int | None = None,
    ) -> None :
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
            handler_id   = handler_id,
        )
        self.ingested = []
        self.storage  = _StorageStub(manifest)
        self.init_machine()

    def apply_message_to_state_machine( self, message : Message) -> None :
        self.ingested.append(message)
        self.state = "ready"

    def process_message( self, _message, _media_content = None) -> bool :
        return False

    def run_while_in_action( self, _max_tokens = None) -> bool :
        return False


class _AgentContextHandler (_StateHandler) :

    AGENT_NAMES = ( "image", "main" )

    def apply_message_to_state_machine( self, message : Message) -> None :
        self.agent_context_clear("image")
        self.agent_context_append( "image", message)
        self.agent_context_append( "main", message)
        self.state = "ready"


class _CallbackHandler (_StateHandler) :

    @classmethod
    def define_state_machine_config(cls) :
        return (
            [
                CaseHandlerState( "start", on_exit = "leave_start"),
                CaseHandlerState(
                    "ready",
                    on_enter = "enter_ready",
                    while_in = "respond",
                ),
            ],
            "start",
            [
                {
                    "source"  : "start",
                    "trigger" : "advance",
                    "dest"    : "ready",
                }
            ],
        )

    def __init__( self, manifest : CaseManifest) -> None :
        super().__init__(manifest)
        self.callback_events = []

    def apply_message_to_state_machine( self, message : Message) -> None :
        self.ingested.append(message)
        self.trigger("advance")
        return

    def leave_start(self) -> None :
        self.callback_events.append(
            (
                "exit",
                list(self.storage.insert_states),
                self.case_manifest.machine_state,
            )
        )
        return

    def enter_ready(self) -> None :
        self.callback_events.append(
            (
                "enter",
                list(self.storage.insert_states),
                self.case_manifest.machine_state,
            )
        )
        return


class _ReplyCallbackHandler (_CallbackHandler) :

    AGENT_NAMES = ( "main", )

    def apply_message_to_state_machine( self, message : Message) -> None :
        if message.text == "inbound" :
            self.agent_context_append( "main", message)
        super().apply_message_to_state_machine(message)
        return

    def leave_start(self) -> None :
        super().leave_start()
        self.agent_context_clear("main")
        return

    def enter_ready(self) -> None :
        super().enter_ready()
        self.apply_and_persist_message(ServerTextMsg(text = "reply"))
        return


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

    def run_while_in_action( self, _max_tokens = None) -> bool :
        return False


class _AsyncIDHandler(AsyncCaseHandlerBase) :

    async def process_message( self, _message) -> bool :
        return False

    async def run_while_in_action( self, _max_tokens = None) -> bool :
        return False


class _AsyncRouteStorage :

    def __init__( self, manifest : CaseManifest) -> None :
        self.manifest          = manifest
        self.inserted_handler_ids = []

    async def get_open_case_manifest( self, _contact : int) -> CaseManifest :
        return self.manifest

    async def update_case_manifest(
        self,
        manifest : CaseManifest,
    ) -> CaseManifest :
        self.manifest = manifest
        return manifest

    async def insert_case_manifest(
        self,
        handler_id    : int | None,
        contact       : int,
        machine_state : str | None,
    ) -> CaseManifest :
        self.inserted_handler_ids.append(handler_id)
        self.manifest = CaseManifest(
            id            = 12,
            contact       = contact,
            handler_id    = handler_id,
            machine_state = machine_state,
        )
        return self.manifest


class _InboundMediaStorage :

    def __init__( self) -> None :
        self.inserted_media = []

    def case_handler_message_exists( self, _inbound_msg_id : int) -> bool :
        return False

    def link_case_handler_to_api(
        self,
        _case_handler_msg_id : int,
        *,
        api_inbound_msg_id : int,
    ) -> bool :
        return bool(api_inbound_msg_id)

    def insert_media( self, **kwargs) -> None :
        self.inserted_media.append(kwargs)


class _MediaWriteStorage :

    def __init__( self) -> None :
        self.calls = []

    def media_write( self, *args) -> str :
        self.calls.append(args)
        return "15551234567/593995341161/2_3.pdf"


class _InboundMediaHandler (WhatsAppCaseHandler) :

    def case_decide(self) -> tuple[ int, CaseManifest] :
        manifest = CaseManifest(
            id         = 71,
            contact    = 31,
            case_index = 2,
        )
        return manifest.id, manifest

    def apply_and_persist_message( self, message : Message) -> Message :
        return message.model_copy(
            update = {
                "id"            : 51,
                "message_index" : 3,
            }
        )

    def process_message( self, _message : Message) -> bool :
        return False

    def run_while_in_action( self, _max_tokens : int | None = None) -> bool :
        return False


class _AsyncInboundMediaStorage :

    def __init__( self) -> None :
        self.inserted_media = []

    async def case_handler_message_exists( self, _inbound_msg_id : int) -> bool :
        return False

    async def link_case_handler_to_api(
        self,
        _case_handler_msg_id : int,
        *,
        api_inbound_msg_id : int,
    ) -> bool :
        return bool(api_inbound_msg_id)

    async def insert_media( self, **kwargs) -> None :
        self.inserted_media.append(kwargs)


class _AsyncMediaWriteStorage :

    def __init__( self) -> None :
        self.calls = []

    async def media_write( self, *args) -> str :
        self.calls.append(args)
        return "15551234567/593995341161/2_3.pdf"


class _AsyncInboundMediaHandler (AsyncWhatsAppCaseHandler) :

    async def case_decide(self) -> tuple[ int, CaseManifest] :
        manifest = CaseManifest(
            id         = 71,
            contact    = 31,
            case_index = 2,
        )
        return manifest.id, manifest

    async def apply_and_persist_message( self, message : Message) -> Message :
        return message.model_copy(
            update = {
                "id"            : 51,
                "message_index" : 3,
            }
        )

    async def process_message( self, _message : Message) -> bool :
        return False

    async def run_while_in_action( self, _max_tokens : int | None = None) -> bool :
        return False


def _manifest( machine_state : str = "ready") -> CaseManifest :
    return CaseManifest(
        id            = 11,
        contact       = 31,
        created_at    = datetime.now(UTC),
        is_open       = True,
        machine_state = machine_state,
    )


def _media_message() -> WhatsAppMessage :
    return WhatsAppMessage.model_validate({
        "from"      : "593995341161",
        "id"        : "wamid.document",
        "timestamp" : "1788724265",
        "type"      : "document",
        "document"  : {
            "id"        : "123456789",
            "mime_type" : "application/pdf",
            "sha256"    : "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            "filename"  : "invoice.pdf",
        },
    })


def test_context_build_restores_fsm_without_replay() -> None :
    handler                  = _StateHandler(_manifest())
    handler.storage.messages = [ ServerTextMsg( id = 50, text = "old") ]

    handler.context_build()

    assert handler.state == "ready"
    assert handler.ingested == []
    assert len(handler.case_context) == 1


def test_state_callbacks_run_after_triggering_message_is_persisted() -> None :
    handler               = _CallbackHandler(_manifest("start"))
    handler.case_id       = handler.storage.manifest.id
    handler.case_manifest = handler.storage.manifest
    handler.case_context  = []

    stored = handler.apply_and_persist_message(ServerTextMsg(text = "new"))

    assert stored.id == 51
    assert handler.state == "ready"
    assert handler.machine.get_state("ready").while_in == [ "respond" ]
    assert handler.callback_events == [
        ( "exit", [ "ready" ], "ready"),
        ( "enter", [ "ready" ], "ready"),
    ]


def test_on_enter_reply_is_persisted_after_triggering_message() -> None :
    handler               = _ReplyCallbackHandler(_manifest("start"))
    handler.case_id       = handler.storage.manifest.id
    handler.case_manifest = handler.storage.manifest
    handler.case_context  = []

    handler.apply_and_persist_message(ServerTextMsg(text = "inbound"))

    assert handler.storage.inserted_texts == [ "inbound", "reply" ]
    assert handler.storage.insert_states == [ "ready", "ready" ]
    assert handler.storage.context_changes == [
        ( [], [ "main" ] ),
        ( [ "main" ], [] ),
    ]
    assert handler.agent_contexts["main"] == []


def test_async_state_callbacks_are_explicitly_delayed() -> None :
    class Model :

        def __init__(self) -> None :
            self.events = []

        async def leave_start(self) -> None :
            self.events.append("exit")

        async def enter_ready(self) -> None :
            self.events.append("enter")

    model   = Model()
    machine = Async_CH_StateMachine(
        [
            CaseHandlerState( "start", on_exit = "leave_start"),
            CaseHandlerState( "ready", on_enter = "enter_ready"),
        ],
        "start",
        [
            {
                "source"  : "start",
                "trigger" : "advance",
                "dest"    : "ready",
            }
        ],
        {
            "leave_start" : model.leave_start,
            "enter_ready" : model.enter_ready,
        },
    )

    async def run_transition() -> None :
        assert await machine.trigger("advance")
        assert machine.state == "ready"
        assert model.events == []

        await machine.run_pending_state_callbacks()

    asyncio.run(run_transition())

    assert not hasattr( machine, "model")
    assert model.events == [ "exit", "enter" ]
    assert asyncio.run(machine.trigger("advance")) is False


def test_apply_and_persist_message_stores_resulting_fsm_state() -> None :
    handler               = _StateHandler(_manifest("start"))
    handler.case_id       = handler.storage.manifest.id
    handler.case_manifest = handler.storage.manifest
    handler.case_context  = []

    stored = handler.apply_and_persist_message(ServerTextMsg(text = "new"))

    assert handler.ingested
    assert handler.storage.insert_states == [ "ready" ]
    assert stored.id == 51
    assert handler.case_manifest.machine_state == "ready"
    assert handler.case_manifest.message_ids == [ 51 ]
    assert handler.case_context == [ stored ]


def test_apply_and_persist_message_stores_agent_context_changes() -> None :
    handler               = _AgentContextHandler(_manifest("start"))
    handler.case_id       = handler.storage.manifest.id
    handler.case_manifest = handler.storage.manifest
    handler.case_context  = []

    stored = handler.apply_and_persist_message(ServerTextMsg(text = "new"))

    assert handler.storage.context_changes == [
        ( [ "image" ], [ "image", "main" ] )
    ]
    assert handler.agent_contexts["image"] == [ stored ]
    assert handler.agent_contexts["main"] == [ stored ]


def test_context_build_restores_current_agent_contexts_without_replay() -> None :
    handler                  = _AgentContextHandler(_manifest())
    image                    = ServerTextMsg( id = 50, text = "image")
    main                     = ServerTextMsg( id = 51, text = "main")
    handler.storage.messages = [ image, main ]
    handler.storage.agent_contexts = {
        "image" : [ 50 ],
        "main"  : [ 51 ],
    }
    handler.ingested = []

    handler.context_build( truncate = False)

    assert handler.agent_contexts == {
        "image" : [ image ],
        "main"  : [ main ],
    }
    assert handler.ingested == []


def test_case_registration_initializes_declared_agent_contexts() -> None :
    handler = _AgentContextHandler(_manifest("start"))

    handler.case_open_new()

    assert handler.storage.inserted_agent_names == [ [ "image", "main" ] ]


def test_handler_change_closes_open_case_and_starts_clean() -> None :
    manifest            = _manifest()
    manifest.handler_id = 8
    handler             = _StateHandler( manifest, handler_id = 7)

    case_id, current = handler.case_decide()

    assert case_id == 12
    assert current.handler_id == 7
    assert handler.storage.inserted_handler_ids == [ 7 ]


def test_async_handler_change_starts_clean_case() -> None :
    manifest             = _manifest()
    manifest.handler_id  = 8
    handler              = object.__new__(_AsyncIDHandler)
    handler.storage      = _AsyncRouteStorage(manifest)
    handler.contact_id   = 31
    handler.handler_id   = 7
    handler.case_id      = None
    handler.case_manifest = None
    handler.machine      = None
    handler.state        = None

    case_id, current = asyncio.run(handler.case_decide())

    assert case_id == 12
    assert current.handler_id == 7
    assert handler.storage.inserted_handler_ids == [ 7 ]


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


def test_whatsapp_media_write_uses_contact_and_message_ordinals() -> None :
    handler               = object.__new__(_InboundMediaHandler)
    handler.storage       = _InboundMediaStorage()
    handler.media_storage = _MediaWriteStorage()
    handler.operator_num  = "15551234567"
    handler.user_id       = "593995341161"

    stored = handler.dedup_and_ingest_message(
        _media_message(),
        media_content      = b"document bytes",
        api_inbound_msg_id = 81,
    )

    assert stored
    assert stored.message_index == 3
    business_phone, contact_user, case_index, message_index, media = (
        handler.media_storage.calls[0]
    )
    assert business_phone == "15551234567"
    assert contact_user == "593995341161"
    assert case_index == 2
    assert message_index == 3
    assert media.extension == "pdf"
    assert handler.storage.inserted_media[0]["object_key"] == (
        "15551234567/593995341161/2_3.pdf"
    )


def test_async_whatsapp_media_write_uses_contact_and_message_ordinals() -> None :
    handler               = object.__new__(_AsyncInboundMediaHandler)
    handler.storage       = _AsyncInboundMediaStorage()
    handler.media_storage = _AsyncMediaWriteStorage()
    handler.operator_num  = "15551234567"
    handler.user_id       = "593995341161"

    stored = asyncio.run(
        handler.dedup_and_ingest_message(
            _media_message(),
            media_content      = b"document bytes",
            api_inbound_msg_id = 81,
        )
    )

    assert stored
    assert stored.message_index == 3
    business_phone, contact_user, case_index, message_index, media = (
        handler.media_storage.calls[0]
    )
    assert business_phone == "15551234567"
    assert contact_user == "593995341161"
    assert case_index == 2
    assert message_index == 3
    assert media.extension == "pdf"
    assert handler.storage.inserted_media[0]["object_key"] == (
        "15551234567/593995341161/2_3.pdf"
    )


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
