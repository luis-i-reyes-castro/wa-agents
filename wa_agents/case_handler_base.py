"""
Case-handler base classes.
"""

from abc import (
    ABC,
    abstractmethod,
)
from base64 import b64decode
from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    NonNegativeInt,
    TypeAdapter,
    model_validator,
)
from types import SimpleNamespace
from typing import (
    Any,
    Self,
    TypedDict,
)
from transitions import (
    Machine,
    State,
)
from transitions.extensions.asyncio import (
    AsyncMachine,
    AsyncState,
)
from uuid import (
    UUID,
    uuid4,
)

from sofia_utils.printing import get_qualname as here
from sofia_utils.pydantic import (
    NE_str,
    NumericID,
)

from .case_handler_models import (
    AssistantMsg,
    CaseManifest,
    HumanContentMsg,
    HumanMsg,
    HumanServerContentMsg,
    HumanServerInteractiveReplyMsg,
    HumanUserContentMsg,
    HumanUserInteractiveReplyMsg,
    LanguageRegionData,
    MediaObject,
    Message,
    ServerInteractiveOptsMsg,
    ServerTemplateMsg,
    ServerTextMsg,
    ToolResultsMsg,
    UserData,
    llm_context_truncate,
)
from .supabase import (
    AsyncSupabaseStorage,
    SyncSupabaseStorage,
)
from .whatsapp_functions import (
    WhatsAppSendResult,
    async_send_whatsapp_interactive,
    async_send_whatsapp_template,
    async_send_whatsapp_text,
    send_whatsapp_interactive,
    send_whatsapp_template,
    send_whatsapp_text,
)
from .whatsapp_models import (
    WhatsAppContact,
    WhatsAppContactPayload,
    WhatsAppMessage,
    WhatsAppMessageEcho,
    WhatsAppMetaData,
)


class TransitionDict (TypedDict) :
    """
    State-machine transition definition.
    """

    source  : str
    trigger : str
    dest    : str


class WhatsAppDatabaseRecord (BaseModel) :
    """
    Normalized WhatsApp business or contact record:
        `row_id`: column `id` in the corresponding table
        `api_id`: the business/contact API ID for sending messages
    Structure for businesses:
        `row_id` : `wa_api_businesses.id`
        `api_id` : `wa_api_businesses.phone_number_id` | null
    Structure for contacts:
        `row_id` : `wa_api_contacts.id`
        `api_id` : `wa_api_contacts.wa_id` | `wa_api_contacts.user_id` | null
    """
    model_config = ConfigDict( frozen = False)
    
    row_id : NonNegativeInt
    api_id : NE_str = "<PHONE_NUMBER_ID|WA_ID|USER_ID>"


class WhatsAppDatabaseRecord_Business ( WhatsAppDatabaseRecord, WhatsAppMetaData) :
    
    model_config = ConfigDict( frozen = False)
    
    waba_id : NumericID
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        self.api_id = self.phone_number_id
        return self


class WhatsAppDatabaseRecord_Contact ( WhatsAppDatabaseRecord, WhatsAppContact) :
    
    model_config = ConfigDict( frozen = False)
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        self.api_id = self.wa_id or self.user_id
        return self


# =========================================================================================
# SYNC CASE HANDLER BASE CLASS
# =========================================================================================

class CH_State (State) :
    """
    State with manually dispatched `while_in` actions. \
    Use `on_enter` / `on_exit` for true FSM callbacks that should run only when a
    transition changes the active state. Use `while_in` for response-generation
    actions that must still run when the handler ingests a message and remains in
    the same state.

    This distinction matters because `CaseHandlerBase` initializes
    `transitions.Machine` with `auto_transitions = False`. In that setup, a user
    message may be ingested without firing any transition, so the machine stays in
    the same state and `on_enter` is not called again. `while_in` actions are
    therefore dispatched manually from `generate_response()`.

    If we instead used `auto_transitions = True` to force a same-state transition,
    we would also need to account for `on_exit` + `on_enter` firing for that same
    state. Keeping `while_in` separate avoids that coupling.
    """

    def __init__(
        self,
        name : str,
        *,
        on_enter                : str | list[str] | None = None,
        while_in                : str | list[str] | None = None,
        on_exit                 : str | list[str] | None = None,
        ignore_invalid_triggers : bool | None            = None,
        final                   : bool                   = False,
    ) -> None :
        super().__init__(
            name,
            on_enter                = on_enter,
            on_exit                 = on_exit,
            ignore_invalid_triggers = ignore_invalid_triggers,
            final                   = final,
        )
        
        if while_in is None :
            self.while_in = []
        elif isinstance( while_in, str) :
            self.while_in = [ while_in ]
        else :
            self.while_in = list(while_in)
        
        return


class CaseHandlerBase ( Machine, ABC) :
    """
    Synchronous case, context, FSM, and persistence base class.
    """

    MAX_CONTEXT_LEN : int | None = 20
    """ Maximum number of LLM-readable messages retained in context. """

    TIME_LIMIT_STALE : int | None = 48
    """ Open-case staleness threshold in hours. """

    def __init__(
        self,
        operator : WhatsAppDatabaseRecord_Business,
        user     : WhatsAppDatabaseRecord_Contact,
        *,
        api_inbound_msg_id : int | None   = None,
        owner_token        : UUID | str | None = None,
        debug              : bool              = False,
        database_url       : str | None        = None,
    ) -> None :
        """
        Initialize a handler for one normalized WhatsApp contact. \
        Args:
            operator          : WhatsApp business phone metadata
            user              : WhatsApp contact data
            api_inbound_msg_id: Current `wa_api_inbound_messages.id`, if any
            owner_token       : Token owning the contact lease
            debug             : Whether to send verbose WhatsApp copies
            database_url      : Optional PostgreSQL URL override
        """
        self.business_id        = operator.row_id
        self.contact_id         = user.row_id
        self.api_inbound_msg_id = api_inbound_msg_id
        self.owner_token        = owner_token or uuid4()

        self.operator_num = operator.display_phone_number
        self.operator_id  = operator.api_id
        self.user_id      = user.api_id
        self.user_name    = user.profile.name if user.profile else None
        self.debug        = debug

        profile = user.profile
        self.user_data = (
            UserData(
                id           = user.row_id,
                name         = profile.name,
                username     = profile.username,
                lan_reg_data = (
                    LanguageRegionData.from_phone_number(user.wa_id)
                    if user.wa_id else None
                ),
            )
            if profile else None
        )

        self.case_id       : int | None          = None
        self.case_manifest : CaseManifest | None = None
        self.case_context  : list[Message] | None = None
        self.machine       : Machine | None       = None
        self.state         : str | None           = None

        self.storage       = SyncSupabaseStorage(database_url)
        self.media_storage : Any | None = None

        self._pending_outbound_msg_ids : dict[ int, list[int] ] = {}

        return

    # =====================================================================================
    # STATE MACHINE

    @classmethod
    def define_state_machine_config(cls) -> tuple[
        list[CH_State],
        str,
        list[TransitionDict],
    ] :
        """
        Overload this method to define state-machine states and transitions. \
        Returns:
            * List of states. Each state must have `name`; `on_enter`, `while_in`,
              and `on_exit` are optional.
            * Initial state name.
            * List of transitions with keys `source`, `trigger`, and `dest`.
        """
        return [], str(None), []

    def init_machine( self, **machine_kwargs) -> None :
        """
        Initialize this handler as a `transitions.Machine` model. \
        Args:
            machine_kwargs : Extra keyword arguments forwarded to `Machine`.
        """
        states, initial, transitions = self.define_state_machine_config()
        states                       = ensure_homogeneous_states(states)
        attach_state_callbacks( self, states)

        Machine.__init__(
            self,
            model                   = self,
            states                  = states,
            initial                 = initial,
            transitions             = transitions,
            auto_transitions        = False,
            ignore_invalid_triggers = True,
            **machine_kwargs,
        )
        self.machine = self

        if self.case_manifest :
            self._restore_machine_state(self.case_manifest)

        return

    @classmethod
    def draw_state_machine_graph(
        cls,
        filename : str | None = "state_machine.png",
    ) -> None :
        states, initial, transitions = cls.define_state_machine_config()

        return draw_state_machine_graph(
            states      = ensure_homogeneous_states(states),
            transitions = transitions,
            initial     = initial,
            filename    = filename,
            class_name  = cls.__name__,
        )

    def ingest_message( self, message : Message) -> None :
        """
        Overload to ingest one message and fire state-machine triggers.
        """
        return

    def reset_state_machine(self) -> None :
        """
        Overload to reset non-FSM handler state when needed.
        """
        return

    def _machine_state(self) -> str | None :
        state = getattr( self, "state", None)
        return state if isinstance( state, str) and state != "None" else None

    def _restore_machine_state( self, manifest : CaseManifest) -> None :
        if self.machine and manifest.machine_state :
            self.machine.set_state( manifest.machine_state, model = self)
        return

    # =====================================================================================
    # CONTACT LEASES

    def acquire_contact_lease(self) -> bool :
        return bool(
            self.storage.acquire_contact_lease( self.contact_id, self.owner_token)
        )

    def renew_contact_lease(self) -> bool :
        return bool(
            self.storage.renew_contact_lease( self.contact_id, self.owner_token)
        )

    def release_contact_lease(self) -> bool :
        return self.storage.release_contact_lease(
            self.contact_id,
            self.owner_token,
        )

    # =====================================================================================
    # CASE MANAGEMENT

    def case_decide(self) -> tuple[int, CaseManifest] :
        """
        Continue the open case when current; otherwise create a new case.
        """
        manifest = self.storage.get_open_case_manifest(self.contact_id)

        if manifest and self.TIME_LIMIT_STALE :
            last = manifest.updated_at or manifest.created_at
            age  = datetime.now(UTC) - last

            if age > timedelta( hours = self.TIME_LIMIT_STALE) :
                manifest.is_open = False
                self.storage.update_case_manifest(manifest)
                manifest = None

        if not manifest :
            return self.case_open_new()

        self.case_id       = manifest.id
        self.case_manifest = manifest
        self._restore_machine_state(manifest)

        return manifest.id, manifest

    def case_open_new(self) -> tuple[int, CaseManifest] :
        """
        Insert and return a new open case for this contact.
        """
        manifest = self.storage.insert_case_manifest(
            self.contact_id,
            self._machine_state(),
        )

        if not manifest :
            manifest = self.storage.get_open_case_manifest(self.contact_id)
        if not manifest :
            raise RuntimeError(
                f"In {here()}: Unable to create or retrieve an open case"
            )

        self.case_id       = manifest.id
        self.case_manifest = manifest
        self._restore_machine_state(manifest)

        return manifest.id, manifest

    def case_mark_as_resolved(self) -> None :
        """
        Close the active case and persist its final FSM state.
        """
        if not self.case_manifest :
            self.case_decide()
        if not self.case_manifest :
            raise RuntimeError(f"In {here()}: No active case manifest")

        self.case_manifest.is_open       = False
        self.case_manifest.machine_state = self._machine_state()

        manifest = self.storage.update_case_manifest(self.case_manifest)
        if not manifest :
            raise RuntimeError(f"In {here()}: Unable to close the active case")

        self.case_manifest = manifest
        return

    # =====================================================================================
    # CONTEXT

    def _get_media_storage(self) -> Any :
        if self.media_storage is None :
            from .S3_bucket_storage import S3BucketStorage

            self.media_storage = S3BucketStorage()

        return self.media_storage

    def _hydrate_media( self, message : Message) -> None :
        if not (
            isinstance( message, HumanContentMsg) and
            message.id and
            message.media
        ) :
            return

        media_rows = self.storage.get_case_handler_media(message.id)
        if not media_rows :
            return

        media_row            = media_rows[0]
        message.media.mime   = media_row["mime_type"]
        message.media.size   = media_row["size"]
        message.media.name   = media_row["filename"] or message.media.name
        message.media.content = self._get_media_storage().media_read(
            media_row["object_key"]
        )

        return

    def context_build( self, truncate : bool = True) -> None :
        """
        Load ordered case messages and restore the persisted FSM state. \
        The machine state comes directly from the case manifest; prior messages are
        not replayed through `ingest_message()`.
        """
        if not ( self.case_id and self.case_manifest ) :
            self.case_id, self.case_manifest = self.case_decide()

        self.case_context = self.storage.get_case_handler_messages(self.case_id)
        if truncate :
            self.case_context = llm_context_truncate(
                self.case_context,
                self.MAX_CONTEXT_LEN,
            )

        for message in self.case_context :
            self._hydrate_media(message)

        self._restore_machine_state(self.case_manifest)
        return

    def context_update( self, message : Message) -> Message :
        """
        Persist one message, its resulting FSM state, and in-memory context. \
        The message is ingested first so the state stored alongside it is the state
        resulting from that message.
        """
        if not ( self.case_id and self.case_manifest ) :
            self.case_id, self.case_manifest = self.case_decide()

        if self.machine :
            self.ingest_message(message)

        machine_state = self._machine_state()
        stored        = self.storage.insert_case_handler_message(
            self.case_id,
            message,
            machine_state,
        )
        if not stored or stored.id is None :
            raise RuntimeError(
                f"In {here()}: Unable to persist case-handler message"
            )

        self._link_pending_outbound_messages( message, stored.id)

        self.case_manifest.machine_state = machine_state
        self.case_manifest.updated_at    = datetime.now(UTC)
        if stored.id not in self.case_manifest.message_ids :
            self.case_manifest.message_ids.append(stored.id)

        if self.case_context is not None :
            self.case_context.append(stored)

        return stored

    # =====================================================================================
    # MESSAGE DEDUPLICATION AND INGESTION

    def dedup_and_ingest_message(
        self,
        message            : WhatsAppMessage | WhatsAppMessageEcho,
        media_content      : bytes | None = None,
        api_inbound_msg_id : int | None   = None,
    ) -> HumanMsg | None :
        """
        Convert and persist one inbound API message unless already linked. \
        Args:
            message            : Validated WhatsApp webhook message.
            media_content      : Fetched media bytes, when applicable.
            api_inbound_msg_id : Persisted inbound API message ID. Defaults to the
                                 ID supplied when the handler was initialized.
        Returns:
            Persisted human message, or `None` when already processed or unsupported.
        """
        inbound_msg_id = api_inbound_msg_id or self.api_inbound_msg_id
        if inbound_msg_id is None :
            raise ValueError(
                f"In {here()}: api_inbound_msg_id is required for message ingestion"
            )
        if self.storage.case_handler_message_exists(inbound_msg_id) :
            return None

        self.case_id, self.case_manifest = self.case_decide()

        ContentMsgBM = (
            HumanUserContentMsg
            if ( not isinstance( message, WhatsAppMessageEcho) ) else
            HumanServerContentMsg
        )
        InteractiveMsgBM = (
            HumanUserInteractiveReplyMsg
            if ( not isinstance( message, WhatsAppMessageEcho) ) else
            HumanServerInteractiveReplyMsg
        )
        msg : HumanMsg | None = None
        if message.text or message.media_data :
            text  = message.text.body if message.text else None
            media = None

            if message.media_data :
                if media_content is None :
                    raise ValueError(
                        f"In {here()}: media_content is required for a media message"
                    )

                media_data = message.media_data
                media      = MediaObject(
                    mime    = media_data.mime_type,
                    name    = f"{media_data.id}.{media_data.extension}",
                    sha256  = b64decode(media_data.sha256).hex(),
                    size    = len(media_content),
                    content = media_content,
                )
                text = media_data.caption

            msg = ContentMsgBM(
                origin = here(),
                ts     = message.timestamp,
                text   = text,
                media  = media,
            )

        elif message.interactive :
            choice = message.interactive.choice
            if not choice :
                raise ValueError(
                    f"In {here()}: Interactive message has no selected option"
                )

            msg = InteractiveMsgBM(
                origin = here(),
                ts     = message.timestamp,
                choice = choice,
            )

        elif message.contacts :
            msg = ContentMsgBM(
                origin = here(),
                ts     = message.timestamp,
                text   = (
                    TypeAdapter( tuple[ WhatsAppContactPayload, ...] )
                    .dump_json(message.contacts).decode()
                ),
            )

        elif message.location :
            msg = ContentMsgBM(
                origin = here(),
                ts     = message.timestamp,
                text   = message.location.model_dump_json(),
            )

        if not msg :
            return None

        msg.print()
        stored = self.context_update(msg)
        if not isinstance( stored, HumanMsg) or stored.id is None :
            raise RuntimeError(
                f"In {here()}: Stored inbound message has an invalid model"
            )

        link = self.storage.link_case_handler_to_api(
            stored.id,
            api_inbound_msg_id = inbound_msg_id,
        )
        if not link :
            raise RuntimeError(
                f"In {here()}: Unable to link case-handler and inbound messages"
            )

        if isinstance( stored, HumanContentMsg) and stored.media :
            stored.media.content = media_content
            object_key = self._get_media_storage().media_write(
                self.business_id,
                self.contact_id,
                self.case_id,
                stored.media,
            )
            self.storage.insert_media(
                inbound_msg_id = inbound_msg_id,
                mime_type      = stored.media.mime,
                size           = stored.media.size or 0,
                object_key     = object_key,
                caption        = stored.text,
                filename       = stored.media.name,
            )

        return stored

    # =====================================================================================
    # MESSAGE SENDING

    def _link_outbound_messages(
        self,
        case_handler_msg_id : int,
        api_outbound_ids    : list[int],
    ) -> None :
        
        for api_outbound_id in api_outbound_ids :
            
            link = self.storage.link_case_handler_to_api(
                case_handler_msg_id,
                api_outbound_msg_id = api_outbound_id,
            )
            if not link :
                raise RuntimeError(
                    f"In {here()}: Unable to link case-handler and outbound messages"
                )
        
        return
    
    def _link_pending_outbound_messages(
        self,
        message             : Message,
        case_handler_msg_id : int,
    ) -> None :
        
        pending = self._pending_outbound_msg_ids.pop( id(message), [])
        self._link_outbound_messages( case_handler_msg_id, pending)
        
        return
    
    def _persist_outbound_messages(
        self,
        message : Message,
        results : list[WhatsAppSendResult],
    ) -> None :
        
        outbound_ids : list[int] = []
        
        for result in results :
            row = self.storage.insert_outbound_message(
                contact  = self.contact_id,
                msg_id   = result["msg_id"],
                msg_type = result["msg_type"],
                msg_data = result["msg_data"],
            )
            if not row :
                raise RuntimeError(
                    f"In {here()}: Unable to persist outbound message"
                )
            outbound_ids.append(row["id"])
        
        if message.id is not None :
            self._link_outbound_messages( message.id, outbound_ids)
        elif outbound_ids :
            self._pending_outbound_msg_ids.setdefault( id(message), [] ).extend(
                outbound_ids
            )
        
        return
    
    def send_template( self, message : ServerTemplateMsg) -> bool :
        """
        Send one WhatsApp template message.
        """
        try :
            results = send_whatsapp_template(
                self.operator_id,
                self.user_id,
                message,
            )
            self._persist_outbound_messages( message, results)
            return True
        except Exception as ex :
            print(f"In {here()}: {str(ex)}")
        return False

    def send_text(
        self,
        message : ServerTextMsg | AssistantMsg | ToolResultsMsg,
    ) -> bool :
        """
        Send normal text or verbose debugging artifacts through WhatsApp.
        """
        try :
            results : list[WhatsAppSendResult] = []
            if (
                isinstance( message, ( ServerTextMsg, AssistantMsg)) and
                message.text
            ) :
                results.extend(
                    send_whatsapp_text(
                        self.operator_id,
                        self.user_id,
                        message.text,
                    )
                )
            self._persist_outbound_messages( message, results)
            return True

        except Exception as ex :
            print(f"In {here()}: {str(ex)}")

        return False

    def send_interactive( self, message : ServerInteractiveOptsMsg) -> bool :
        """
        Send one WhatsApp interactive message or its debugging variant.
        """
        try :
            results = send_whatsapp_interactive(
                self.operator_id,
                self.user_id,
                message,
            )
            self._persist_outbound_messages( message, results)
            return True

        except Exception as ex :
            print(f"In {here()}: {str(ex)}")

        return False

    # =====================================================================================
    # CHILD HANDLER INTERFACE

    @abstractmethod
    def process_message(
        self,
        message       : WhatsAppMessage,
        media_content : bytes | None = None,
    ) -> bool :
        """
        Process one inbound WhatsApp message. \
        Returns:
            `True` if additional response generation is required; otherwise `False`.
        """
        raise NotImplementedError

    @abstractmethod
    def generate_response(
        self,
        max_tokens : int | None = None,
    ) -> bool :
        """
        Generate one assistant response pass. \
        Args:
            max_tokens : Optional response-token limit.
        Returns:
            `True` if another response pass is required; otherwise `False`.
        """
        raise NotImplementedError


# =========================================================================================
# ASYNC CASE HANDLER BASE CLASS
# =========================================================================================

class Async_CH_State (AsyncState) :
    """
    Async counterpart to `CH_State`; it uses the same `while_in` semantics.
    """
    
    def __init__(
        self,
        name : str,
        *,
        on_enter                : str | list[str] | None = None,
        while_in                : str | list[str] | None = None,
        on_exit                 : str | list[str] | None = None,
        ignore_invalid_triggers : bool | None            = None,
        final                   : bool                   = False,
    ) -> None :
        super().__init__(
            name,
            on_enter                = on_enter,
            on_exit                 = on_exit,
            ignore_invalid_triggers = ignore_invalid_triggers,
            final                   = final,
        )
        
        if while_in is None :
            self.while_in = []
        elif isinstance( while_in, str) :
            self.while_in = [ while_in ]
        else :
            self.while_in = list(while_in)
        
        return


class AsyncCaseHandlerBase ( AsyncMachine, ABC) :
    """
    Asynchronous case, context, FSM, and persistence base class.
    """

    MAX_CONTEXT_LEN : int | None = 20
    """ Maximum number of LLM-readable messages retained in context. """

    TIME_LIMIT_STALE : int | None = 48
    """ Open-case staleness threshold in hours. """

    def __init__(
        self,
        operator : WhatsAppDatabaseRecord_Business,
        user     : WhatsAppDatabaseRecord_Contact,
        *,
        api_inbound_msg_id : int | None   = None,
        owner_token        : UUID | str | None = None,
        debug              : bool              = False,
        database_url       : str | None        = None,
    ) -> None :
        """
        Initialize an asynchronous handler for one normalized WhatsApp contact. \
        Args:
            operator          : WhatsApp business phone metadata.
            user              : WhatsApp contact data.
            api_inbound_msg_id: Current `wa_api_inbound_messages.id`, if any.
            owner_token       : Token owning the contact lease.
            debug             : Whether to send verbose WhatsApp copies.
            database_url      : Optional PostgreSQL URL override.
        """
        self.business_id        = operator.row_id
        self.contact_id         = user.row_id
        self.api_inbound_msg_id = api_inbound_msg_id
        self.owner_token        = owner_token or uuid4()

        self.operator_num = operator.display_phone_number
        self.operator_id  = operator.api_id
        self.user_id      = user.api_id
        self.user_name    = user.profile.name if user.profile else None
        self.debug        = debug

        profile = user.profile
        self.user_data = (
            UserData(
                id           = user.row_id,
                name         = profile.name,
                username     = profile.username,
                lan_reg_data = (
                    LanguageRegionData.from_phone_number(user.wa_id)
                    if user.wa_id else None
                ),
            )
            if profile else None
        )

        self.case_id       : int | None          = None
        self.case_manifest : CaseManifest | None = None
        self.case_context  : list[Message] | None = None
        self.machine       : AsyncMachine | None  = None
        self.state         : str | None            = None

        self.storage       = AsyncSupabaseStorage(database_url)
        self.media_storage : Any | None = None

        self._pending_outbound_msg_ids : dict[ int, list[int] ] = {}

        return

    # =====================================================================================
    # STATE MACHINE

    @classmethod
    def define_state_machine_config(cls) -> tuple[
        list[ CH_State | Async_CH_State ],
        str,
        list[TransitionDict],
    ] :
        """
        Overload this method to define state-machine states and transitions. \
        Returns:
            * List of synchronous or asynchronous states. Each state must have
              `name`; `on_enter`, `while_in`, and `on_exit` are optional.
            * Initial state name.
            * List of transitions with keys `source`, `trigger`, and `dest`.
        """
        return [], str(None), []

    def init_machine( self, **machine_kwargs) -> None :
        """
        Initialize this handler as a `transitions.AsyncMachine` model. \
        Args:
            machine_kwargs : Extra keyword arguments forwarded to `AsyncMachine`.
        """
        states, initial, transitions = self.define_state_machine_config()
        async_states                 = to_async_states(states)
        attach_state_callbacks( self, async_states)

        AsyncMachine.__init__(
            self,
            model                   = self,
            states                  = async_states,
            initial                 = initial,
            transitions             = transitions,
            auto_transitions        = False,
            ignore_invalid_triggers = True,
            **machine_kwargs,
        )
        self.machine = self

        if self.case_manifest :
            self._restore_machine_state(self.case_manifest)

        return

    @classmethod
    def draw_state_machine_graph(
        cls,
        filename : str | None = "state_machine.png",
    ) -> None :
        states, initial, transitions = cls.define_state_machine_config()

        return draw_state_machine_graph(
            states      = to_async_states(states),
            transitions = transitions,
            initial     = initial,
            filename    = filename,
            class_name  = cls.__name__,
        )

    async def ingest_message( self, message : Message) -> None :
        """
        Overload to ingest one message and fire state-machine triggers.
        """
        return

    def reset_state_machine(self) -> None :
        """
        Overload to reset non-FSM handler state when needed.
        """
        return

    def _machine_state(self) -> str | None :
        state = getattr( self, "state", None)
        return state if isinstance( state, str) and state != "None" else None

    def _restore_machine_state( self, manifest : CaseManifest) -> None :
        if self.machine and manifest.machine_state :
            self.machine.set_state( manifest.machine_state, model = self)
        return

    # =====================================================================================
    # CONTACT LEASES

    async def acquire_contact_lease(self) -> bool :
        row = await self.storage.acquire_contact_lease(
            self.contact_id,
            self.owner_token,
        )
        return bool(row)

    async def renew_contact_lease(self) -> bool :
        row = await self.storage.renew_contact_lease(
            self.contact_id,
            self.owner_token,
        )
        return bool(row)

    async def release_contact_lease(self) -> bool :
        return await self.storage.release_contact_lease(
            self.contact_id,
            self.owner_token,
        )

    # =====================================================================================
    # CASE MANAGEMENT

    async def case_decide(self) -> tuple[int, CaseManifest] :
        """
        Continue the open case when current; otherwise create a new case.
        """
        manifest = await self.storage.get_open_case_manifest(self.contact_id)

        if manifest and self.TIME_LIMIT_STALE :
            last = manifest.updated_at or manifest.created_at
            age  = datetime.now(UTC) - last

            if age > timedelta( hours = self.TIME_LIMIT_STALE) :
                manifest.is_open = False
                await self.storage.update_case_manifest(manifest)
                manifest = None

        if not manifest :
            return await self.case_open_new()

        self.case_id       = manifest.id
        self.case_manifest = manifest
        self._restore_machine_state(manifest)

        return manifest.id, manifest

    async def case_open_new(self) -> tuple[int, CaseManifest] :
        """
        Insert and return a new open case for this contact.
        """
        manifest = await self.storage.insert_case_manifest(
            self.contact_id,
            self._machine_state(),
        )

        if not manifest :
            manifest = await self.storage.get_open_case_manifest(self.contact_id)
        if not manifest :
            raise RuntimeError(
                f"In {here()}: Unable to create or retrieve an open case"
            )

        self.case_id       = manifest.id
        self.case_manifest = manifest
        self._restore_machine_state(manifest)

        return manifest.id, manifest

    async def case_mark_as_resolved(self) -> None :
        """
        Close the active case and persist its final FSM state.
        """
        if not self.case_manifest :
            await self.case_decide()
        if not self.case_manifest :
            raise RuntimeError(f"In {here()}: No active case manifest")

        self.case_manifest.is_open       = False
        self.case_manifest.machine_state = self._machine_state()

        manifest = await self.storage.update_case_manifest(self.case_manifest)
        if not manifest :
            raise RuntimeError(f"In {here()}: Unable to close the active case")

        self.case_manifest = manifest
        return

    # =====================================================================================
    # CONTEXT

    def _get_media_storage(self) -> Any :
        if self.media_storage is None :
            from .S3_bucket_storage import AsyncS3BucketStorage

            self.media_storage = AsyncS3BucketStorage()

        return self.media_storage

    async def _hydrate_media( self, message : Message) -> None :
        if not (
            isinstance( message, HumanContentMsg) and
            message.id and
            message.media
        ) :
            return

        media_rows = await self.storage.get_case_handler_media(message.id)
        if not media_rows :
            return

        media_row             = media_rows[0]
        message.media.mime    = media_row["mime_type"]
        message.media.size    = media_row["size"]
        message.media.name    = media_row["filename"] or message.media.name
        message.media.content = await self._get_media_storage().media_read(
            media_row["object_key"]
        )

        return

    async def context_build( self, truncate : bool = True) -> None :
        """
        Load ordered case messages and restore the persisted FSM state. \
        The machine state comes directly from the case manifest; prior messages are
        not replayed through `ingest_message()`.
        """
        if not ( self.case_id and self.case_manifest ) :
            self.case_id, self.case_manifest = await self.case_decide()

        self.case_context = await self.storage.get_case_handler_messages(self.case_id)
        if truncate :
            self.case_context = llm_context_truncate(
                self.case_context,
                self.MAX_CONTEXT_LEN,
            )

        for message in self.case_context :
            await self._hydrate_media(message)

        self._restore_machine_state(self.case_manifest)
        return

    async def context_update( self, message : Message) -> Message :
        """
        Persist one message, its resulting FSM state, and in-memory context. \
        The message is ingested first so the state stored alongside it is the state
        resulting from that message.
        """
        if not ( self.case_id and self.case_manifest ) :
            self.case_id, self.case_manifest = await self.case_decide()

        if self.machine :
            await self.ingest_message(message)

        machine_state = self._machine_state()
        stored        = await self.storage.insert_case_handler_message(
            self.case_id,
            message,
            machine_state,
        )
        if not stored or stored.id is None :
            raise RuntimeError(
                f"In {here()}: Unable to persist case-handler message"
            )

        await self._link_pending_outbound_messages( message, stored.id)

        self.case_manifest.machine_state = machine_state
        self.case_manifest.updated_at    = datetime.now(UTC)
        if stored.id not in self.case_manifest.message_ids :
            self.case_manifest.message_ids.append(stored.id)

        if self.case_context is not None :
            self.case_context.append(stored)

        return stored

    # =====================================================================================
    # MESSAGE DEDUPLICATION AND INGESTION

    async def dedup_and_ingest_message(
        self,
        message            : WhatsAppMessage | WhatsAppMessageEcho,
        media_content      : bytes | None = None,
        api_inbound_msg_id : int | None   = None,
    ) -> HumanMsg | None :
        """
        Convert and persist one inbound API message unless already linked. \
        Args:
            message            : Validated WhatsApp webhook message.
            media_content      : Fetched media bytes, when applicable.
            api_inbound_msg_id : Persisted inbound API message ID. Defaults to the
                                 ID supplied when the handler was initialized.
        Returns:
            Persisted human message, or `None` when already processed or unsupported.
        """
        inbound_msg_id = api_inbound_msg_id or self.api_inbound_msg_id
        if inbound_msg_id is None :
            raise ValueError(
                f"In {here()}: api_inbound_msg_id is required for message ingestion"
            )
        if await self.storage.case_handler_message_exists(inbound_msg_id) :
            return None

        self.case_id, self.case_manifest = await self.case_decide()

        ContentMsgBM = (
            HumanUserContentMsg
            if ( not isinstance( message, WhatsAppMessageEcho) ) else
            HumanServerContentMsg
        )
        InteractiveMsgBM = (
            HumanUserInteractiveReplyMsg
            if ( not isinstance( message, WhatsAppMessageEcho) ) else
            HumanServerInteractiveReplyMsg
        )
        msg : HumanMsg | None = None
        if message.text or message.media_data :
            text  = message.text.body if message.text else None
            media = None

            if message.media_data :
                if media_content is None :
                    raise ValueError(
                        f"In {here()}: media_content is required for a media message"
                    )

                media_data = message.media_data
                media      = MediaObject(
                    mime    = media_data.mime_type,
                    name    = f"{media_data.id}.{media_data.extension}",
                    sha256  = b64decode(media_data.sha256).hex(),
                    size    = len(media_content),
                    content = media_content,
                )
                text = media_data.caption

            msg = ContentMsgBM(
                origin = here(),
                ts     = message.timestamp,
                text   = text,
                media  = media,
            )

        elif message.interactive :
            choice = message.interactive.choice
            if not choice :
                raise ValueError(
                    f"In {here()}: Interactive message has no selected option"
                )

            msg = InteractiveMsgBM(
                origin = here(),
                ts     = message.timestamp,
                choice = choice,
            )

        elif message.contacts :
            msg = ContentMsgBM(
                origin = here(),
                ts     = message.timestamp,
                text   = (
                    TypeAdapter( tuple[ WhatsAppContactPayload, ...] )
                    .dump_json(message.contacts).decode()
                ),
            )

        elif message.location :
            msg = ContentMsgBM(
                origin = here(),
                ts     = message.timestamp,
                text   = message.location.model_dump_json(),
            )

        if not msg :
            return None

        msg.print()
        stored = await self.context_update(msg)
        if not isinstance( stored, HumanMsg) or stored.id is None :
            raise RuntimeError(
                f"In {here()}: Stored inbound message has an invalid model"
            )

        link = await self.storage.link_case_handler_to_api(
            stored.id,
            api_inbound_msg_id = inbound_msg_id,
        )
        if not link :
            raise RuntimeError(
                f"In {here()}: Unable to link case-handler and inbound messages"
            )

        if isinstance( stored, HumanContentMsg) and stored.media :
            stored.media.content = media_content
            object_key = await self._get_media_storage().media_write(
                self.business_id,
                self.contact_id,
                self.case_id,
                stored.media,
            )
            await self.storage.insert_media(
                inbound_msg_id = inbound_msg_id,
                mime_type      = stored.media.mime,
                size           = stored.media.size or 0,
                object_key     = object_key,
                caption        = stored.text,
                filename       = stored.media.name,
            )

        return stored

    # =====================================================================================
    # MESSAGE SENDING

    async def _link_outbound_messages(
        self,
        case_handler_msg_id : int,
        api_outbound_ids    : list[int],
    ) -> None :
        
        for api_outbound_id in api_outbound_ids :
            
            link = await self.storage.link_case_handler_to_api(
                case_handler_msg_id,
                api_outbound_msg_id = api_outbound_id,
            )
            if not link :
                raise RuntimeError(
                    f"In {here()}: Unable to link case-handler and outbound messages"
                )
        
        return
    
    async def _link_pending_outbound_messages(
        self,
        message             : Message,
        case_handler_msg_id : int,
    ) -> None :
        
        pending = self._pending_outbound_msg_ids.pop( id(message), [])
        await self._link_outbound_messages( case_handler_msg_id, pending)
        
        return
    
    async def _persist_outbound_messages(
        self,
        message : Message,
        results : list[WhatsAppSendResult],
    ) -> None :
        
        outbound_ids : list[int] = []
        
        for result in results :
            row = await self.storage.insert_outbound_message(
                contact  = self.contact_id,
                msg_id   = result["msg_id"],
                msg_type = result["msg_type"],
                msg_data = result["msg_data"],
            )
            if not row :
                raise RuntimeError(
                    f"In {here()}: Unable to persist outbound message"
                )
            outbound_ids.append(row["id"])
        
        if message.id is not None :
            await self._link_outbound_messages( message.id, outbound_ids)
        elif outbound_ids :
            self._pending_outbound_msg_ids.setdefault( id(message), [] ).extend(
                outbound_ids
            )
        
        return
    
    async def send_template( self, message : ServerTemplateMsg) -> bool :
        """
        Send one WhatsApp template message asynchronously.
        """
        try :
            results = await async_send_whatsapp_template(
                self.operator_id,
                self.user_id,
                message,
            )
            await self._persist_outbound_messages( message, results)
            return True
        except Exception as ex :
            print(f"In {here()}: {str(ex)}")
        return False

    async def send_text(
        self,
        message : ServerTextMsg | AssistantMsg | ToolResultsMsg,
    ) -> bool :
        """
        Send normal text or verbose debugging artifacts asynchronously.
        """
        try :
            results : list[WhatsAppSendResult] = []
            if (
                isinstance( message, ( ServerTextMsg, AssistantMsg)) and
                message.text
            ) :
                results.extend(
                    await async_send_whatsapp_text(
                        self.operator_id,
                        self.user_id,
                        message.text,
                    )
                )
            await self._persist_outbound_messages( message, results)
            return True

        except Exception as ex :
            print(f"In {here()}: {str(ex)}")

        return False

    async def send_interactive( self, message : ServerInteractiveOptsMsg) -> bool :
        """
        Send one WhatsApp interactive message asynchronously.
        """
        try :
            results = await async_send_whatsapp_interactive(
                self.operator_id,
                self.user_id,
                message,
            )
            await self._persist_outbound_messages( message, results)
            return True

        except Exception as ex :
            print(f"In {here()}: {str(ex)}")

        return False

    # =====================================================================================
    # CHILD HANDLER INTERFACE

    @abstractmethod
    async def process_message(
        self,
        message       : WhatsAppMessage,
        media_content : bytes | None = None,
    ) -> bool :
        """
        Process one inbound WhatsApp message asynchronously. \
        Returns:
            `True` if additional response generation is required; otherwise `False`.
        """
        raise NotImplementedError

    @abstractmethod
    async def generate_response(
        self,
        max_tokens : int | None = None,
    ) -> bool :
        """
        Generate one assistant response pass asynchronously. \
        Args:
            max_tokens : Optional response-token limit.
        Returns:
            `True` if another response pass is required; otherwise `False`.
        """
        raise NotImplementedError


# =========================================================================================
# STATE MACHINE HELPERS
# =========================================================================================

def attach_state_callbacks(
    machine : object,
    states  : list[State] | list[AsyncState],
) -> object :
    """
    Ensure every real `on_enter` / `on_exit` callback exists on `machine`. \
    `while_in` actions are intentionally excluded because they are dispatched
    manually from `generate_response()` and are not FSM callbacks.
    """
    if all( isinstance( state, CH_State) for state in states ) :
        def dummy_callback() -> None :
            return
    else :
        async def dummy_callback() -> None :
            return

    for state in states :
        for callback_name in state.on_enter :
            if not hasattr( machine, callback_name) :
                setattr( machine, callback_name, dummy_callback)

        for callback_name in state.on_exit :
            if not hasattr( machine, callback_name) :
                setattr( machine, callback_name, dummy_callback)

    return machine


def draw_state_machine_graph(
    *,
    states      : list[State] | list[AsyncState],
    initial     : str | None,
    transitions : list[TransitionDict],
    filename    : str        = "state_machine.png",
    class_name  : str | None = None,
) -> None :
    """
    Draw the state-machine graph.
    * States are rounded rectangles with black borders.
    * State labels distinguish `on_enter`, `while_in`, and `on_exit` callbacks.
    * States containing "agent" in their name are orange.
    * Transition arrows are red and their labels are blue.

    `GraphMachine` requires a graphing engine. It is deliberately not included in
    `pyproject.toml` because it is not needed in production. Install Graphviz with
    `sudo apt install graphviz`, or install PyGraphviz with
    `pip install pygraphviz` before calling this helper.
    """
    import re
    from transitions.extensions import GraphMachine

    dummy_model   = attach_state_callbacks( SimpleNamespace(), states)
    graph_machine = GraphMachine(
        model                   = dummy_model,
        states                  = states,
        initial                 = initial,
        transitions             = transitions,
        auto_transitions        = False,
        ignore_invalid_triggers = True,
        show_state_attributes   = True,
    )

    graph         = graph_machine.get_graph()
    state_by_name = { state.name : state for state in states }

    # Draw the graph from top to bottom.
    graph.graph_attr["rankdir"] = "TB"

    # Include an optional title.
    if class_name :
        graph.graph_attr["label"]    = f"\n{class_name} State Machine\n\n"
        graph.graph_attr["labelloc"] = "t"
        graph.graph_attr["fontsize"] = "16"
        graph.graph_attr["fontname"] = "Courier-Bold"

    # Customize state nodes and labels.
    for node in graph.nodes() :
        if "label" in node.attr :
            label = re.sub( r"^(\w+)", r"STATE '\1'", node.attr["label"])
            state = state_by_name.get(node.name)

            if state and getattr( state, "while_in", []) :
                while_in_lines = "- while_in:\\l" + "".join(
                    f"  + {action}\\l"
                    for action in state.while_in
                )
                if "- exit:\\l" in label :
                    label = label.replace(
                        "- exit:\\l",
                        while_in_lines + "- exit:\\l",
                    )
                else :
                    label += while_in_lines

            label = label.replace( "+", "•")
            label = label.replace( "- enter:",    "[»] on enter:")
            label = label.replace( "- while_in:", "[»] while in:")
            label = label.replace( "- exit:",     "[»] on exit:")

            node.attr["label"]    = label
            node.attr["fontname"] = "Courier"
            node.attr["fontsize"] = "10"

        # Apply the state style, border, and agent-state fill color.
        node.attr["style"] = "rounded,filled"
        node.attr["color"] = "black"
        node.attr["fillcolor"] = (
            "orange" if "agent" in node.name else "white"
        )

    # Style transitions.
    for edge in graph.edges() :
        edge.attr["color"]     = "red"
        edge.attr["fontcolor"] = "blue"
        edge.attr["fontname"]  = "Courier"
        edge.attr["fontsize"]  = "10"

    # Render the graph.
    graph.draw( filename, prog = "dot")
    return


def ensure_homogeneous_states(
    states : list[ CH_State | AsyncState ],
) -> list[CH_State] | list[Async_CH_State] :
    if not (
        all( isinstance( state, CH_State)       for state in states ) or
        all( isinstance( state, Async_CH_State) for state in states )
    ) :
        raise ValueError(f"In {here()}: List of states is not homogeneous")

    return states


def to_async_states(
    states : list[ State | Async_CH_State ],
) -> list[AsyncState] :
    """
        Convert synchronous handler states to async state objects.
        """
    async_states = []

    for state in states :
        if isinstance( state, AsyncState) :
            async_states.append(state)
        else :
            async_states.append(
                Async_CH_State(
                    name                    = state.name,
                    on_enter                = list(state.on_enter),
                    while_in                = list(getattr( state, "while_in", [])),
                    on_exit                 = list(state.on_exit),
                    ignore_invalid_triggers = state.ignore_invalid_triggers,
                    final                   = state.final,
                )
            )

    return async_states
