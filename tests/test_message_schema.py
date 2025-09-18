"""
Unit tests for message bus schema validation and performatives
"""

import pytest
import asyncio
from datetime import datetime
from uuid import uuid4

from bus import Message, Performative, Router


class TestMessageSchema:
    """Test message schema validation"""

    def test_message_creation_valid(self):
        """Test valid message creation"""
        message = Message.create(
            performative=Performative.REQUEST,
            sender="agent1",
            receiver="agent2",
            conversation_id="conv-123",
            content_type="query",
            content={"question": "test"}
        )

        assert message.performative == Performative.REQUEST
        assert message.sender == "agent1"
        assert message.receiver == "agent2"
        assert message.conversation_id == "conv-123"
        assert message.content_type == "query"
        assert message.content == {"question": "test"}
        assert isinstance(message.timestamp, datetime)
        # Message doesn't have id field in current implementation

    def test_message_performatives(self):
        """Test all performative types"""
        performatives = [
            Performative.REQUEST,
            Performative.INFORM,
            Performative.FAILURE,
            Performative.PROPOSE,
            Performative.AGREE
        ]

        for perf in performatives:
            message = Message.create(
                performative=perf,
                sender="test",
                receiver="test",
                conversation_id="test",
                content_type="test",
                content={}
            )
            assert message.performative == perf

    def test_message_reply_correlation(self):
        """Test reply correlation preserves conversation_id"""
        original = Message.create(
            performative=Performative.REQUEST,
            sender="agent1",
            receiver="agent2",
            conversation_id="conv-123",
            content_type="query",
            content={"question": "test"}
        )

        reply = original.reply(
            performative=Performative.INFORM,
            content_type="response",
            content={"answer": "test"}
        )

        assert reply.conversation_id == original.conversation_id
        assert reply.sender == original.receiver  # Swapped
        assert reply.receiver == original.sender  # Swapped
        assert reply.performative == Performative.INFORM

    def test_conversation_id_correlation(self):
        """Test that multiple messages can share conversation_id"""
        conv_id = str(uuid4())

        msg1 = Message.create(
            performative=Performative.REQUEST,
            sender="agent1",
            receiver="agent2",
            conversation_id=conv_id,
            content_type="query",
            content={"step": 1}
        )

        msg2 = Message.create(
            performative=Performative.INFORM,
            sender="agent2",
            receiver="agent3",
            conversation_id=conv_id,
            content_type="forward",
            content={"step": 2}
        )

        msg3 = Message.create(
            performative=Performative.INFORM,
            sender="agent3",
            receiver="agent1",
            conversation_id=conv_id,
            content_type="response",
            content={"step": 3}
        )

        # All messages should have same conversation_id
        assert msg1.conversation_id == conv_id
        assert msg2.conversation_id == conv_id
        assert msg3.conversation_id == conv_id


class TestRouterMessaging:
    """Test router message handling"""

    @pytest.fixture
    def router(self):
        return Router()

    @pytest.mark.asyncio
    async def test_agent_registration(self, router):
        """Test agent registration and unregistration"""
        inbox = asyncio.Queue()

        # Register agent
        router.register_agent("test_agent", inbox)
        assert "test_agent" in router._agents

        # Unregister agent
        router.unregister_agent("test_agent")
        assert "test_agent" not in router._agents

    @pytest.mark.asyncio
    async def test_message_routing(self, router):
        """Test message routing between agents"""
        inbox1 = asyncio.Queue()
        inbox2 = asyncio.Queue()

        router.register_agent("agent1", inbox1)
        router.register_agent("agent2", inbox2)

        # Send message from agent1 to agent2
        message = Message.create(
            performative=Performative.REQUEST,
            sender="agent1",
            receiver="agent2",
            conversation_id="test-conv",
            content_type="ping",
            content={"data": "hello"}
        )

        await router.route(message)

        # agent2 should receive the message
        received = await asyncio.wait_for(inbox2.get(), timeout=1.0)
        assert received.sender == "agent1"
        assert received.receiver == "agent2"
        assert received.content["data"] == "hello"
        assert received.conversation_id == "test-conv"

    @pytest.mark.asyncio
    async def test_message_routing_unknown_receiver(self, router):
        """Test routing to unknown receiver"""
        message = Message.create(
            performative=Performative.REQUEST,
            sender="agent1",
            receiver="unknown_agent",
            conversation_id="test-conv",
            content_type="ping",
            content={}
        )

        # Should not raise exception, just log warning
        await router.route(message)

    @pytest.mark.asyncio
    async def test_conversation_flow(self, router):
        """Test complete conversation flow with correlation"""
        inbox1 = asyncio.Queue()
        inbox2 = asyncio.Queue()

        router.register_agent("requester", inbox1)
        router.register_agent("responder", inbox2)

        # Step 1: Initial request
        request = Message.create(
            performative=Performative.REQUEST,
            sender="requester",
            receiver="responder",
            conversation_id="conv-456",
            content_type="query",
            content={"question": "What is 2+2?"}
        )

        await router.route(request)
        received_request = await asyncio.wait_for(inbox2.get(), timeout=1.0)

        # Step 2: Response
        response = received_request.reply(
            performative=Performative.INFORM,
            content_type="answer",
            content={"result": 4}
        )

        await router.route(response)
        received_response = await asyncio.wait_for(inbox1.get(), timeout=1.0)

        # Verify conversation correlation
        assert request.conversation_id == received_request.conversation_id
        assert received_request.conversation_id == received_response.conversation_id
        assert received_response.content["result"] == 4

    @pytest.mark.asyncio
    async def test_failure_performative(self, router):
        """Test FAILURE performative handling"""
        inbox1 = asyncio.Queue()
        inbox2 = asyncio.Queue()

        router.register_agent("client", inbox1)
        router.register_agent("server", inbox2)

        # Client sends request
        request = Message.create(
            performative=Performative.REQUEST,
            sender="client",
            receiver="server",
            conversation_id="fail-test",
            content_type="invalid_query",
            content={"malformed": True}
        )

        await router.route(request)
        received_request = await asyncio.wait_for(inbox2.get(), timeout=1.0)

        # Server responds with failure
        failure = received_request.reply(
            performative=Performative.FAILURE,
            content_type="error",
            content={"error": "Invalid query format", "code": 400}
        )

        await router.route(failure)
        received_failure = await asyncio.wait_for(inbox1.get(), timeout=1.0)

        # Verify failure handling
        assert received_failure.performative == Performative.FAILURE
        assert received_failure.content["error"] == "Invalid query format"
        assert received_failure.conversation_id == request.conversation_id


class TestMessageTimestamps:
    """Test message timestamp handling"""

    def test_message_timestamp_creation(self):
        """Test that messages get timestamps on creation"""
        before = datetime.now()

        message = Message.create(
            performative=Performative.INFORM,
            sender="test",
            receiver="test",
            conversation_id="test",
            content_type="test",
            content={}
        )

        after = datetime.now()

        assert before <= message.timestamp <= after

    def test_message_ordering_by_timestamp(self):
        """Test that messages can be ordered by timestamp"""
        msg1 = Message.create(
            performative=Performative.REQUEST,
            sender="test",
            receiver="test",
            conversation_id="test",
            content_type="first",
            content={}
        )

        # Small delay to ensure different timestamps
        import time
        time.sleep(0.001)

        msg2 = Message.create(
            performative=Performative.INFORM,
            sender="test",
            receiver="test",
            conversation_id="test",
            content_type="second",
            content={}
        )

        assert msg1.timestamp < msg2.timestamp


if __name__ == "__main__":
    pytest.main([__file__])