# Python Imports
from enum import Enum


class MessageContentType(Enum):
    UNKNOWN_CONTENT_TYPE = 0
    TEXT_PLAIN = 1
    STICKER = 2
    STATUS = 3
    EMOJI = 4
    TRANSACTION_COMMAND = 5
    SYSTEM_MESSAGE_CONTENT_PRIVATE_GROUP = 6
    IMAGE = 7
    AUDIO = 8
    COMMUNITY = 9
    SYSTEM_MESSAGE_GAP = 10
    CONTACT_REQUEST = 11
    DISCORD_MESSAGE = 12
    IDENTITY_VERIFICATION = 13
    SYSTEM_MESSAGE_PINNED_MESSAGE = 14
    SYSTEM_MESSAGE_MUTUAL_EVENT_SENT = 15
    SYSTEM_MESSAGE_MUTUAL_EVENT_ACCEPTED = 16
    SYSTEM_MESSAGE_MUTUAL_EVENT_REMOVED = 17
    BRIDGE_MESSAGE = 18

class SignalType(Enum):
    MESSAGES_NEW = "messages.new"
    MESSAGE_DELIVERED = "message.delivered"
    ENVELOPE_SENT = "envelope.sent"
    ENVELOPE_EXPIRED = "envelope.expired"
    HISTORY_REQUEST_STARTED = "history.request.started"
    HISTORY_REQUEST_COMPLETED = "history.request.completed"
    LOCAL_NOTIFICATIONS = "local-notifications"
    NODE_READY = "node.ready"
    NODE_STARTED = "node.started"
    NODE_LOGIN = "node.login"
    NODE_LOGOUT = "node.stopped"
    COMMUNITY_IMPORTING_HISTORY_ARCHIVE_MESSAGES_FINISHED = "community.downloadingHistoryArchivesFinished"
