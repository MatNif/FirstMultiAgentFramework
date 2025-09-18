from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class Performative(Enum):
    REQUEST = "request"
    INFORM = "inform"
    PROPOSE = "propose"
    AGREE = "agree"
    FAILURE = "failure"


@dataclass
class Message:
    performative: Performative
    sender: str
    receiver: str
    conversation_id: str
    content_type: str
    content: Dict[str, Any]
    timestamp: datetime

    def __post_init__(self) -> None:
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp)

    @classmethod
    def create(
        cls,
        performative: Performative,
        sender: str,
        receiver: str,
        conversation_id: str,
        content_type: str,
        content: Dict[str, Any],
        timestamp: Optional[datetime] = None,
    ) -> "Message":
        if timestamp is None:
            timestamp = datetime.now()

        return cls(
            performative=performative,
            sender=sender,
            receiver=receiver,
            conversation_id=conversation_id,
            content_type=content_type,
            content=content,
            timestamp=timestamp,
        )

    def reply(
        self,
        performative: Performative,
        content_type: str,
        content: Dict[str, Any],
        sender: Optional[str] = None,
    ) -> "Message":
        return Message.create(
            performative=performative,
            sender=sender or self.receiver,
            receiver=self.sender,
            conversation_id=self.conversation_id,
            content_type=content_type,
            content=content,
        )