# -*- coding: utf-8 -*-

import sys
import threading
import time
import socket
import asyncore
import asynchat
import logging
from functools import partial
from enum import Enum

BYTEORDER = 'little'
MAGIC_BYTES =  bytes((0x2F, 0x47, 0x61, 0x71, 0x95, 0xAD, 0xC5, 0xFB))
END_PAYLOAD_MAGIC_BYTES = int.to_bytes(0x10F13467, 4, BYTEORDER)
HEADER_LENGTH = 8 # bytes
DEFAULT_ENCODING = 'utf-8'
DEFAULT_STREAM_BODY_BUFFER_SIZE = 16 * 1024
SOCK_SELECT_TIMEOUT = 1 # seconds

icHeartBeat = -4;
icEndSession = -5;
icFlushQueue = -6;
icUniqueClientID = -7;
icTimeStamp = -8;
icEvent = -15;
icEndClientSession = -21;
icFlushClientQueue = -22;
icConnectToGateway = -23;
icSetClientInfo = -31;
icSetVariable = -32;
icAllVariables = -33;
icSetState = -34;
icSetThrottle = -35;
icSetNoDelay = -36;
icSetVariablePrefixed = -37;
icRequestEventNames = -41;
icEventNames = -42;
icRequestSubscribers = -43;
icRequestPublishers = -44;
icSubscribe = -45;
icUnsubscribe = -46;
icPublish = -47;
icUnpublish = -48;
icSetEventIDTranslation = -49;
icStatusEvent = -52;
icStatusClient = -53;
icStatusEventPlus = -54;
icStatusClientPlus = -55;
icStatusHUB = -56;
icStatusTimer = -57;
icHumanReadableHeader = -60;
icSetMonitor = -61;
icResetMonitor = -62;
icCreateTimer = -73;
icHUBLocate = -81;
icHUBFound = -82;
icLogClear = -91;
icLogRequest = -92;
icLogContents = -93;

actionNew = 0;
actionDelete = 1;
actionChange = 2;

ekChangeObjectEvent = 0;
ekStreamHeader = 1;
ekStreamBody = 2;
ekStreamTail = 3;
ekBuffer = 4;
ekNormalEvent = 5;
ekChangeObjectDataEvent = 6;
ekChildEventAdd = 11;
ekChildEventRemove = 12;
ekLogWriteLn = 30;
ekTimerCancel = 40;
ekTimerPrepare = 41;
ekTimerStart = 42;
ekTimerStop = 43;
ekTimerAcknowledgedListAdd = 45;
ekTimerAcknowledgedListRemove = 46;
ekTimerSetSpeed = 47;
ekTimerTick = 48;
ekTimerAcknowledge = 49;
ekTimerStatusRequest = 50;

logging.basicConfig(format='[%(levelname)8s] %(message)s', level=logging.DEBUG)


def encode_int32(value):
    """Encode a signed 32-bit integer, using the default byte order.

    Args:
        value (int): The integer to encode.

    Returns:
        bytes: A `bytes` object representing the integer.
    """
    return value.to_bytes(4, BYTEORDER, signed=True)

def decode_int(buf):
    """Decode a signed integer from bytes, using the default byte order.

    Args:
        buf (bytes or similar): The bytes to decode.

    Returns:
        int: The decoded integer.
    """
    return int.from_bytes(buf, BYTEORDER, signed=True)

def encode_uint32(value):
    """Encode an unsigned 32-bit integer in bytes using the default byte order.

    Args:
        value (int): The integer to encode.

    Returns:
        bytes: A `bytes` object representing the integer.
    """
    return value.to_bytes(4, BYTEORDER, signed=False)

def decode_uint(buf):
    """Decode an unsigned integer from bytes, using the default byte order.

    Args:
        buf (bytes or similar): The bytes to decode.

    Returns:
        int: The decoded integer.
    """
    return int.from_bytes(buf, BYTEORDER, signed=False)

def encode_string(value):
    """Encode a string (prefixed with string length).

    Strings are encoded as follows. First comes a signed 32-bit integer equal 
    to the number of bytes in the encoded string. Then comes the string, encoded
    with the default encoding.

    Args:
        value (str): The string to encode.

    Returns:
        bytes: A `bytes` object representing the integer.
    """
    encoded = value.encode(DEFAULT_ENCODING)
    return b''.join([
        encode_int32(len(encoded)),
        encoded])

def decode_string(buf, start=0, nextpos=False):
    """Decode a string prefixed with length.

    This function is the reverse of :func:`encode_string`.

    Args:
        buf (bytes or similar): The bytes to decode.
        start (int, optional): The index to start decoding from in the bytes.
        nextpos (bool, optional): If `True`, the function will return a 
            tuple `(decoded_string, nextpos)`, where `nextpos` is equal to 
            one plus the index of the last byte of the string. (See example below.)

    Examples:
        >>> encode_string('foo')
        b'\x03\x00\x00\x00foo'

        >>> decode_string(encode_string('bar'))
        'bar'

        >>> buf = b''.join([encode_string('foo'), encode_string('bar')])
        >>> s1, nextpos = decode_string(buf, nextpos=True)
        >>> s1
        'foo'
        >>> nextpos
        7
        >>> s2 = decode_string(buf, start=nextpos)
        >>> s2
        'bar'
    """
    length = decode_int(buf[start:(start+4)])
    string = buf[(start+4):(start+4+length)].decode(DEFAULT_ENCODING)
    if nextpos:
        return string, start+length+4
    else:
        return string

class EventDefinition(object):
    """Represents an event in the IMB framework

    The :class:`EventDefinition` object represents an event (a named 
    communication channel) in the IMB framework. It is created by a
    :class:`Client` instance and can then be used to

    *   subscribe/unsubscribe to the event,
    *   publish/unpublish the event,
    *   send signals, and
    *   setup handlers for incoming signals.

    Note:
        You should not create EventDefinition instances yourself. 
        Instead, call :func:`Client.get_event`, :func:`Client.subscribe` or 
        :func:`Client.publish`.
    """

    def __init__(self, event_id, name, client):
        """Constructor for `EventDefinition`.

        You should not create EventDefinition instances yourself. 
        Instead, call :func:`Client.get_event`, :func:`Client.subscribe` or 
        :func:`Client.publish`.

        Args:
            event_id (int): The Client's id for the event.
            name (str): The name used to identify the event.
            client (imb.Client): The Client instance to be used.
        """
        super(EventDefinition, self).__init__()

        self.event_id = event_id
        self.name = name
        self._client = client
        self._is_subscribed = False
        self._is_published = False
        self._handlers = {}
        self._streams = {}
        self._create_stream_callback = None
        self._end_stream_callback = None

        self._handlers[ekStreamHeader] = (self._handle_stream_header,)
        self._handlers[ekStreamBody] = (self._handle_stream_body,)
        self._handlers[ekStreamTail] = (self._handle_stream_tail,)

    @property
    def create_stream_callback(self):
        """Property: Callback function for stream creation.

        This property should be either `None` or a callable to be called when
        a stream header arrives on the event. The callback function should take two
        arguments: `(stream_id, stream_name)`, and return a stream object to
        save the incoming stream in.

        If no stream object is returned from the stream callback function,
        the incoming stream will not be saved.

        Note:
            There is also a property :func:`end_stream_callback` which
            is called at the end of the stream, just before it is closed.

        Example:

        >>> def create_stream(stream_id, stream_name):
        ...     filename = str(stream_id) + '_' + stream_name
        ...     return open(filename, 'wb+')
        ...
        >>> def end_stream(stream):
        ...     pass # do something here, just before automatic stream.close()
        ...
        >>> e = client.subscribe('my-stream')
        >>> e.create_stream_callback = create_stream
        >>> e.end_stream_callback = end_stream # this is optional, really

        """
        return self._create_stream_callback

    @create_stream_callback.setter
    def create_stream_callback(self, value):
        self._create_stream_callback = value
    

    @property
    def end_stream_callback(self):
        """Property: Callback function for handling end of stream.

        Similar to :func:`create_stream_callback`, this property should either be `None`
        or a callable to be called when a stream tail has arrived on the event.
        The callback function should take one argument `stream`: it will be passed
        the same stream object that was created in :func:`create_stream_callback`.

        Note:
            The stream is automatically closed right after the `end_stream_callback`
            function is called, so you don't have to do this manually.

        Example:

            See :func:`create_stream_callback` for an example.
        """
        return self._end_stream_callback
    @end_stream_callback.setter
    def end_stream_callback(self, value):
        self._end_stream_callback = value


    def add_handler(self, event_kind, handler):
        """Add a handler for received events of a certain kind.

        The handler is a callable object which is called after an event
        has arrived. Your handler is expected to have the right arguments
        signature for the event kind. Check examples below, or the source of
        :func:`_decode_event_payload` if you are unsure.

        Args:
            event_kind (int): One of the event kind constants, e.g. `ekNormalEvent`
                or `ekChangeObjectEvent`.
            handler (callable): The handler.

        Raises:
            RuntimeError: If you try to add handlers for stream events.

        Example:
            >>> c = imb.Client(url, port, owner_id, owner_name, federation)
            >>> e = c.subscribe('event name')
            >>> def my_change_object_handler(action, object_id, short_event_name, attr_name):
            ...     print('ChangeObjectEvent:', action, object_id, short_event_name, attr_name)
            >>> e.add_handler(imb.ekChangeObjectEvent, my_change_object_handler)
            >>> def my_normal_event_handler(payload):
            ...     print('NormalEvent:', payload)
            >>> e.add_handler(imb.ekNormalEvent, my_normal_event_handler)
        """

        if event_kind in (ekStreamHeader, ekStreamBody, ekStreamTail):
            raise RuntimeError("Don't do that. We'll handle it for you!")

        if not event_kind in self._handlers:
            self._handlers[event_kind] = set()
        self._handlers[event_kind].add(handler)

    def remove_handler(self, handler):
        for event_kind in self._handlers:
            try:
                self._handlers[event_kind].remove(handler)
            except KeyError:
                pass

    @property
    def client(self):
        return self._client

    @property
    def subscribers(self):
        raise NotImplementedError()
    
    @property
    def publishers(self):
        raise NotImplementedError()

    def subscribe(self):
        """Subscribe the owning Client to this event

        Equivalent to `client.signal_subscribe(...)`.

        This function is provided only for convenience."""
        event_entry_type = 0
        if not self._is_subscribed:
            self.client.signal_subscribe(self.event_id, event_entry_type, self.name)
            self._is_subscribed = True

    def unsubscribe(self):
        """Unsubscribe the owning Client from this event

        Equivalent to `client.signal_unsubscribe(...)`.

        This function is provided only for convenience."""
        if self._is_subscribed:
            self.client.signal_unsubscribe(self.name)
            self._is_subscribed = False

    def publish(self):
        """Publish this event with the owning Client

        Equivalent to `client.signal_publish(...)`.

        This function is provided only for convenience."""
        event_entry_type = 0
        if not self._is_published:
            self.client.signal_publish(self.event_id, event_entry_type, self.name)
            self._is_published = True

    def unpublish(self):
        """Unpublish this event with the owning Client

        Equivalent to `client.signal_unpublish(...)`.

        This function is provided only for convenience."""
        if self._is_published:
            self.client.signal_unpublish(self.name)
            self._is_published = False

    def handle_event(self, event_kind, event_payload):
        """Call all handlers registered for the `event_kind`.

        You never have to call this function directly. It is called by the
        owning :class:`Client` object.

        See also :func:`EventDefinition.add_handler`.
        """
        for handler in self._handlers[event_kind]:
            args = self._decode_event_payload(event_kind, event_payload)
            handler(*args)

    def _decode_event_payload(self, event_kind, payload):
        if event_kind == ekNormalEvent:
            return (payload,)

        elif event_kind == ekChangeObjectEvent:
            action = decode_int(payload[0:4])
            object_id = decode_int(payload[4:8])
            short_event_name = self.name
            attr_name = decode_string(payload, 8)
            return (action, object_id, short_event_name, attr_name)

        elif event_kind == ekStreamHeader:
            stream_id = decode_int(payload[0:4])
            stream_name = decode_string(payload, 4)
            return (stream_id, stream_name)

        elif event_kind in (ekStreamBody, ekStreamTail):
            stream_id = decode_int(payload[0:4])
            data = payload[4:]
            return (stream_id, data)

        else:
            raise NotImplementedError()

    def signal_event(self, event_kind, event_payload):
        """Send a message on the event.

        Args:
            event_kind (int): One of the event kind constants, e.g. `ekNormalEvent`
                or `ekChangeObjectEvent`.
            event_payload (bytes or similar): The payload to send.
        """
        if not self._is_published:
            self.publish()
        self.client.signal_normal_event(self.event_id, event_kind, event_payload)

    def signal_change_object(self, action, object_id, attribute):
        """Send a ChangeObject event.

        Args:
            action (int): One of the action constants.
            object_id (int): Id of object to change.
            attribute (string)
        """
        event_payload = b''.join([
            encode_int32(action),
            encode_int32(object_id),
            encode_string(attribute)])

        self.signal_event(ekChangeObjectEvent, event_payload)

    def signal_string(self, value):
        """Send a NormalEvent event with only a string in the payload.

        This is provided for convenience.
        Equivalent to `self.signal_event(ekNormalEvent, encode_string(value))`.

        Args:
            value (str): The string to send.
        """
        event_kind = ekNormalEvent
        payload = encode_string(value)
        self.signal_event(event_kind, payload)

    def _hash_stream(self, name):
        hash64 = hash(name + repr(self.client.socket.getsockname()))
        upper = (hash64 >> 32) & 0x7FFFFFFF
        lower = hash64 & 0x7FFFFFFF

        stream_id = upper ^ lower
        return stream_id


    def signal_stream(self, name, stream, chunk_size=DEFAULT_STREAM_BODY_BUFFER_SIZE):
        """Send a stream on the event.

        Args:
            name (str): A name for the stream, unique for the Client.
            stream (stream object): The stream to be sent. Must be implemented 
                as a standard Python stream, e.g. a file object obtained 
                through `open(filename, 'b')`.
            chunk_size (int, optional): Number of bytes to send in each body chunk.
        """
        stream_id = self._hash_stream(name)

        # header
        parts = [
            encode_int32(stream_id),
            encode_string(name)]
        self.signal_event(ekStreamHeader, b''.join(parts))

        # body or tail
        while True:
            chunk = stream.read(chunk_size)
            payload = b''.join((encode_int32(stream_id), chunk))

            if len(chunk) == chunk_size:
                self.signal_event(ekStreamBody, payload)
            else:
                self.signal_event(ekStreamTail, payload)
                break

    def _handle_stream_header(self, stream_id, stream_name):
        # logging.debug('Handling stream HEAD.')
        if self._create_stream_callback:
            stream = self._create_stream_callback(stream_id, stream_name)
            if stream:
                self._streams[stream_id] = stream

    def _handle_stream_body(self, stream_id, data):
        # logging.debug('Handling stream BODY chunk.')
        self._streams[stream_id].write(data)

    def _handle_stream_tail(self, stream_id, data):
        # logging.debug('Handling stream TAIL chunk.')
        stream = self._streams[stream_id]
        stream.write(data)
        if self._end_stream_callback:
            self._end_stream_callback(stream)
        stream.close()
        del self._streams[stream_id]


class Command(object):
    """Represents a command that can be sent by a Client

    Commands are things like "subscribe to event", "publish event",
    "signal event", etc.

    You don't need to use this class directly. It is used only by 
    the :class:`Client` class."""
    def __init__(self, command_code=None, payload=None):
        super(Command, self).__init__()
        self.command_code = command_code
        self.payload = payload


    @property
    def payload(self):
        return self._payload
    @payload.setter
    def payload(self, value):
        self._payload = value
        self.length = len(value) if value else 0


class ClientStates(Enum):
    """Possible states of a :class:`Client`.

    Used internally by Client.
    """
    waiting = 0
    header = 1
    payload = 2

    
class Client(asynchat.async_chat):
    """A client that can connect to an IMB hub.

    The client tries to open a socket immediately as the object is created.
    The socket read/write logic is implemented with the python `asynchat`
    module, and the `asyncore.loop()` call is made in a separate thread.
    That thread will not finish untill :func:`Client.disconnect` has been
    called.


    Examples:

        >>> import imb
        >>> host = 'localhost'
        >>> port = 4000
        >>> owner_id = 123
        >>> owner_name = 'my name'
        >>> federation = 'my federation'
        >>> 
        >>> c = imb.Client(host, port, owner_id, owner_name, federation) # Connect to a hub
        >>>
        >>> # Example 1: Send a file as a stream
        >>> e = c.publish('my event') # Now we can send signals on the event 
        >>> e.signal_stream('stream name', open('test.txt', 'rb')) # Empty the file stream
        >>> e.unpublish()
        >>>
        >>> Example 2: Receive a stream
        >>> def create_stream(stream_id, stream_name):
        ...     filename = str(stream_id) + '_' + stream_name
        ...     return open(filename, 'wb+')
        ...
        >>> def end_stream(stream):
        ...     pass # do something here, just before automatic stream.close()
        ...
        >>> e = client.subscribe('my-stream')
        >>> e.create_stream_callback = create_stream
        >>> e.end_stream_callback = end_stream # this is optional, really
        >>> # Now just wait for a stream to come flying on the event
        >>> # And finally always disconnect:
        >>> c.disconnect()

    """
    def __init__(self, host, port, owner_id=None, owner_name=None, federation=None):
        
        self._channels_map = {}
        super(Client, self).__init__(map=self._channels_map)

        self._ibuffer = []
        self._set_state(ClientStates.waiting)
        self._command = None

        self._event_id_translation = {} # hub's event ID to client's event ID
        self._event_definitions = {} # event id to EventDefinition object

        self._federation = federation
        self._unique_client_id = None
        
        logging.info(
            'Connecting to {0}:{1}. Owner ID: {2}; Owner name: {3}; Federation: {4}.'.format(
                host, port, owner_id, owner_name, federation))
        
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port)) # connect socket
        t = threading.Thread(
            target=partial(asyncore.loop, map=self._channels_map, timeout=SOCK_SELECT_TIMEOUT))
        t.start()
        
        self._signal_client_info(owner_id, owner_name)

    def _set_state(self, state):
        self._state = state

        if state is ClientStates.waiting:
            self.set_terminator(MAGIC_BYTES)
        elif state is ClientStates.header:
            self.set_terminator(HEADER_LENGTH)
        elif state is ClientStates.payload:
            self.set_terminator(END_PAYLOAD_MAGIC_BYTES)

    @property
    def federation(self):
        return self._federation

    @property
    def unique_client_id(self):
        if not self._unique_client_id:
            # send explicit request for the unique client id (backwards compatible with older hubs)
            self._signal_request_unique_client_id()
            # wait for a max of 10*0.5 seconds = 5 seconds
            spincount = 10
            while (not self._unique_client_id) & (spincount>0):
                time.sleep(0.5)
                spincount -= 1
            if not self._unique_client_id:
                self._unique_client_id = -1 # mark as -1 to signal we could not get it from the hub and wont retry
        return self._unique_client_id
    
    @property
    def client_id(self):
        return self._client_id
    

    def handle_connect(self):
        """Called by async_chat class when the client has connected.

        See docs for async_chat.
        """
        logging.info('Connected to {0}:{1}'.format(*self.socket.getpeername()))
        super(Client, self).handle_connect()

    def collect_incoming_data(self, data):
        """Called by async_chat for incoming data on the TCP socket.

        See docs for async_chat.
        """
        self._ibuffer.append(data)

    def found_terminator(self):
        """Called by async_chat when a terminator sequence is received.

        See docs for async_chat.
        """

        # Start of command
        if self._state is ClientStates.waiting:
            self._ibuffer = []
            self._set_state(ClientStates.header)

        # Received header
        elif self._state is ClientStates.header:
            header = b''.join(self._ibuffer)
            command_code = decode_int(header[0:4])
            payload_length = decode_int(header[4:])
            self._command = Command(command_code=command_code)
            self._ibuffer = []
            if payload_length == 0:
                self._handle_command(self._command)
                self._set_state(ClientStates.waiting)
            else:
                self._set_state(ClientStates.payload)

        # Received payload
        elif self._state is ClientStates.payload:
            self._set_state(ClientStates.waiting)
            self._command.payload = b''.join(self._ibuffer)
            self._handle_command(self._command)
    

    def _handle_command(self, command):

        if command.command_code == icEvent:
            hub_event_id = decode_int(command.payload[0:4])
            client_event_id = self._event_id_translation[hub_event_id]
            # Here, we could get the tick (payload[4:8]), but we don't
            event_kind = decode_uint(command.payload[8:12])
            event_payload = command.payload[12:]
            # logging.debug((
            #     'Received icEvent. Hub id: {0}; Client id: {1}; '
            #     'Event kind: {2}; Payload length: {3}').format(
            #         hub_event_id, client_event_id, event_kind, len(event_payload)))

            self._handle_event(client_event_id, event_kind, event_payload)


        elif command.command_code == icSetEventIDTranslation:
            hub_event_id = decode_int(command.payload[0:4])
            client_event_id = decode_int(command.payload[4:8])
            if client_event_id >= 0:
                self._event_id_translation[hub_event_id] = client_event_id
            else:
                del self._event_id_translation[hub_event_id]

            # logging.debug("Handled icSetEventIDTranslation. Hub's event id: {0}; Client's event id: {1}.".format(
            #     hub_event_id, client_event_id))

        elif command.command_code == icUniqueClientID:

            self._unique_client_id = decode_uint(command.payload[0:4])
            self._client_id = decode_uint(command.payload[4:8])

            # logging.debug('Handled icUniqueClientID. Unique client id: {0}; Client id: {1}'.format(
            #     self._unique_client_id, self._client_id))

        elif command.command_code == icEndSession:
            self.disconnect()

            # logging.debug('Handled icEndSession.')

        else:
            pass

    def _handle_event(self, event_id, event_kind, event_payload):
        event = self._event_definitions[event_id]
        event.handle_event(event_kind, event_payload)

    def disconnect(self):
        """Disconnect the underlying socket."""
        while self.writable():
            logging.debug('There is still data left to write. '
                'Waiting a little before disconnecting...')
            time.sleep(1) # 1 second
        logging.info('Closing connection to {0}:{1}.'.format(*self.socket.getpeername()))
        self.socket.shutdown(socket.SHUT_RDWR)
        self.close()

    def _signal_command(self, message):
        parts = [
            MAGIC_BYTES,
            encode_int32(message.command_code),
            encode_int32(message.length)]

        if message.payload:
            if len(message.payload) > 0:
                parts.append(message.payload)
                parts.append(END_PAYLOAD_MAGIC_BYTES)

        self.push(b''.join(parts))

    def _signal_client_info(self, owner_id, owner_name):
        payload = b''.join([
            encode_int32(owner_id),
            encode_string(owner_name)])

        self._signal_command(Command(command_code=icSetClientInfo, payload=payload))

    def _signal_request_unique_client_id(self):
        payload = b''.join([
            encode_int32(0),
            encode_int32(0)])
        self._signal_command(Command(command_code=icUniqueClientID, payload=payload))
    
    def signal_subscribe(self, event_id, event_entry_type, event_name):
        """Subscribe to an event.

        You probably want to use :func:`Client.subscribe` or 
        :func:`EventDefinition.subscribe` instead.
        """

        payload = b''.join([
            encode_int32(event_id),
            encode_int32(event_entry_type),
            encode_string(event_name)])

        self._signal_command(Command(command_code=icSubscribe, payload=payload))

    def signal_publish(self, event_id, event_entry_type, event_name):
        """Publish an event.

        You probably want to use :func:`Client.publish` or 
        :func:`EventDefinition.publish` instead.
        """
        payload = b''.join([
            encode_int32(event_id),
            encode_int32(event_entry_type),
            encode_string(event_name)])

        self._signal_command(Command(command_code=icPublish, payload=payload))

    def signal_unsubscribe(self, event_name):
        """Unsubscribe from an event.

        You probably want to use :func:`Client.unsubscribe` or 
        :func:`EventDefinition.unsubscribe` instead.
        """
        payload = encode_string(event_name)

        self._signal_command(Command(command_code=icUnsubscribe, payload=payload))

    def signal_unpublish(self, event_name):
        """Unpublish an event.

        You probably want to use :func:`Client.unpublish` or 
        :func:`EventDefinition.unpublish` instead.
        """
        payload = encode_string(event_name)

        self._signal_command(Command(command_code=icUnpublish, payload=payload))

    def signal_normal_event(self, event_id, event_kind, event_payload):
        """Send an `icEvent` command.

        You probably want to use e.g. :func:`Event.signal_event` instead.
        It will at least simplify things by supplying the `event_id` for you.

        Args:
            event_id (int): The event_id to send.
            event_kind (int): One of the event kind constants, e.g. `ekNormalEvent`
                or `ekChangeObjectEvent`.
            event_payload (bytes or similar): The event payload to send along.
        """
        payload = b''.join([
            encode_int32(event_id),
            encode_int32(0),
            encode_int32(event_kind),
            event_payload])

        self._signal_command(Command(command_code=icEvent, payload=payload))

    def _make_new_event_id(self):
        event_id = 0
        while True:
            if not event_id in self._event_definitions:
                return event_id
            else:
                event_id += 1

    def get_event(self, event_name, prefix=True, create=True):
        """Get an EventDefinition object that can be used for communication.

        The :class:`EventDefinition` object returned will not be subscribed
        or published to anything.

        Args:
            event_name (str): The name of the event.
            prefix (bool, optional): If `True`, the event name will be prefixed with
                the client `federation` + `.`, so you get something like
                ` my_federation.my_event_name`.
            create (bool, optional): If `True` (the default), the event will be
                created if it doesn't exist yet. If `False`, and the event doesn't exist
                yet, this function will return `None`.

        Returns:
            An EventDefinition object or None.

        Example:

            >>> c = imb.Client(host, port, owner_id, owner_name, federation) # Connect to a hub
            >>> e = c.get_event('my event')
            >>> e.publish() # Now we can send signals on the event
        """

        if prefix:
            event_name = self.federation + '.' + event_name

        # Look for the event among the existing ones
        for key, event in self._event_definitions.items():
            if event.name == event_name:
                return event

        # If we have come this far, the event has not been created yet
        if create:
            event_id = self._make_new_event_id()
            event = EventDefinition(event_id, event_name, self)
            self._event_definitions[event_id] = event
            return event
        else:
            return None
        

    def subscribe(self, event_name, prefix=True):
        """Get an EventDefinition object and ensure it is subscribed.

        Args:
            event_name (str): See docs for :func:`Client.get_event`.
            prefix (bool, optional): See docs for :func:`Client.get_event`.

        Returns:
            A subscribed EventDefinition object.

        Example:

            >>> e1 = client.get_event('my_event')
            >>> e1.subscribe()
            >>> e2 = c.subscribe('my_event') # This gets a reference to the same object
            >>> e1 is e2
            True
        """

        event = self.get_event(event_name, prefix=prefix)
        event.subscribe()
        return event

    def unsubscribe(self, event_name, prefix=True):
        """Ensure that the client is not subscribed to an event.

        Args:
            event_name (str): See docs for :func:`Client.get_event`.
            prefix (bool, optional): See docs for :func:`Client.get_event`.

        Returns:
            The EventDefinition object, if it already existed. Otherwise `None`.
        """
        event = self.get_event(event_name, prefix=prefix, create=False)
        if event:
            event.unsubscribe()
        return event

    def publish(self, event_name, prefix=True):
        """Get an EventDefinition object and ensure it is subscribed.

        Args:
            event_name (str): See docs for :func:`Client.get_event`.
            prefix (bool, optional): See docs for :func:`Client.get_event`.

        Returns:
            A published EventDefinition object.

        Example:

            >>> # Two equivalent ways of getting a published EventDefinition object
            >>> e1 = client.get_event('my_event')
            >>> e1.publish()
            >>> e2 = c.publish('my_event') # This gets a reference to the same object
            >>> e1 is e2
            True
        """
        event = self.get_event(event_name, prefix=prefix)
        event.publish()
        return event

    def unpublish(self, event_name, prefix=True):
        """Ensure that the client is not publishing an event.

        Args:
            event_name (str): See docs for :func:`Client.get_event`.
            prefix (bool, optional): See docs for :func:`Client.get_event`.

        Returns:
            The EventDefinition object, if it already existed. Otherwise `None`.
        """
        event = self.get_event(event_name, prefix=prefix, create=False)
        if event:
            event.unpublish()
        return event
