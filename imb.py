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
CMD_LENGTH = 4 # bytes
PAYLOAD_SIZE_LENGTH = 4 # bytes
HEADER_LENGTH = CMD_LENGTH + PAYLOAD_SIZE_LENGTH
DEFAULT_ENCODING = 'utf-8'
DEFAULT_STREAM_BODY_BUFFER_SIZE = 16 * 1024

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


TEST_URL = 'imb.lohman-solutions.com'
TEST_PORT = 4000

class ClientStates(Enum):
    """docstring for ClientStates"""
    waiting = 0
    header = 1
    payload = 2

def encode_int32(value):
    return value.to_bytes(4, BYTEORDER, signed=True)

def decode_int32(value):
    return int.from_bytes(value, BYTEORDER, signed=True)

def encode_uint32(value):
    return value.to_bytes(4, BYTEORDER, signed=False)

def decode_uint32(value):
    return int.from_bytes(value, BYTEORDER, signed=False)

def encode_string(value):
    encoded = value.encode(DEFAULT_ENCODING)
    return b''.join([
        encode_int32(len(encoded)),
        encoded])

def decode_string(buf, start, return_endpos=False):
    length = decode_int32(buf[start:(start+4)])
    string = buf[(start+4):(start+4+length)].decode(DEFAULT_ENCODING)
    if return_endpos:
        return string, start+length+4
    else:
        return string

class EventDefinition(object):
    """docstring for EventDefinition"""
    def __init__(self, event_id, name, client):
        super(EventDefinition, self).__init__()
        self.event_id = event_id
        self.name = name
        self._client = client
        self._is_subscribed = False
        self._is_published = False
        self._handlers = {}
        self._streams = {}
        self.create_stream_callback = None
        self.end_stream_callback = None

        self._handlers[ekStreamHeader] = (self._handle_stream_header,)
        self._handlers[ekStreamBody] = (self._handle_stream_body,)
        self._handlers[ekStreamTail] = (self._handle_stream_tail,)


    def add_handler(self, event_kind, handler):
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
        event_entry_type = 0
        if not self._is_subscribed:
            self.client.signal_subscribe(self.event_id, event_entry_type, self.name)
            self._is_subscribed = True

    def unsubscribe(self):
        if self._is_subscribed:
            self.client.signal_unsubscribe(self.name)
            self._is_subscribed = False

    def publish(self):
        event_entry_type = 0
        if not self._is_published:
            self.client.signal_publish(self.event_id, event_entry_type, self.name)
            self._is_published = True

    def unpublish(self):
        if self._is_published:
            self.client.signal_unpublish(self.name)
            self._is_published = False

    def handle_event(self, event_kind, event_payload):
        for handler in self._handlers[event_kind]:
            args = self._decode_event_payload(event_kind, event_payload)
            handler(*args)

    def _decode_event_payload(self, event_kind, payload):
        if event_kind == ekNormalEvent:
            return (payload,)

        elif event_kind == ekChangeObjectEvent:
            action = decode_int32(payload[0:4])
            object_id = decode_int32(payload[4:8])
            short_event_name = self.name
            attr_name = decode_string(payload, 8)
            return (action, object_id, short_event_name, attr_name)

        elif event_kind == ekStreamHeader:
            stream_id = decode_int32(payload[0:4])
            stream_name = decode_string(payload, 4)
            logging.debug('Decoding: stream id: {0}; stream name: {1}'.format(stream_id, stream_name))
            return (stream_id, stream_name)

        elif event_kind in (ekStreamBody, ekStreamTail):
            stream_id = decode_int32(payload[0:4])
            data = payload[4:]
            return (stream_id, data)

        else:
            raise NotImplementedError()

    def signal_event(self, event_kind, event_payload):
        if not self._is_published:
            self.publish()
        self.client.signal_normal_event(self.event_id, event_kind, event_payload)

    def signal_change_object(self, action, object_id, attribute):
        event_payload = b''.join([
            encode_int32(action),
            encode_int32(object_id),
            encode_string(attribute)])

        self.signal_event(ekChangeObjectEvent, event_payload)

    def signal_string(self, value):
        event_kind = ekNormalEvent
        payload = encode_string(value)
        self.signal_event(event_kind, payload)

    def hash_stream(self, name):
        hash64 = hash(name + repr(self.client.socket.getsockname()))
        upper = (hash64 >> 32) & 0x7FFFFFFF
        lower = hash64 & 0x7FFFFFFF

        stream_id = upper ^ lower
        return stream_id


    def signal_stream(self, name, stream, chunk_size=DEFAULT_STREAM_BODY_BUFFER_SIZE):
        stream_id = self.hash_stream(name)
        logging.debug('Signalling stream with stream id {0}'.format(stream_id))

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
        logging.debug('Handling stream HEAD.')
        if self.create_stream_callback:
            stream = self.create_stream_callback(stream_id, stream_name)
            if stream:
                self._streams[stream_id] = stream

    def _handle_stream_body(self, stream_id, data):
        logging.debug('Handling stream BODY chunk.')
        self._streams[stream_id].write(data)

    def _handle_stream_tail(self, stream_id, data):
        logging.debug('Handling stream TAIL chunk.')
        stream = self._streams[stream_id]
        stream.write(data)
        if self.end_stream_callback:
            self.end_stream_callback(self, stream)
        stream.close()
        del self._streams[stream_id]


class Command(object):
    """docstring for Command"""
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
                                                                                                              
    
class Client(asynchat.async_chat):
    """docstring for Client"""
    def __init__(self, host, port, owner_id=None, owner_name=None, federation=None):
        
        self._channels_map = {}
        super(Client, self).__init__(map=self._channels_map)
        
        self._ibuffer = []
        self._state = None
        self._set_state(ClientStates.waiting)
        self._command = None

        self._event_id_translation = {} # hub's event ID to client's event ID
        self._event_definitions = {} # event id to EventDefinition object

        self._federation = federation
        logging.info(
            'Connecting to {0}:{1}. Owner ID: {2}; Owner name: {3}; Federation: {4}.'.format(
                host, port, owner_id, owner_name, federation))
        
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port)) # connect socket
        t = threading.Thread(target=partial(asyncore.loop, map=self._channels_map))
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
        else:
            raise NotImplementedError()

    @property
    def federation(self):
        return self._federation

    @property
    def unique_client_id(self):
        return self._unique_client_id
    
    @property
    def client_id(self):
        return self._client_id
    

    def handle_connect(self):
        logging.info('Connected to {0}:{1}'.format(*self.socket.getpeername()))
        super(Client, self).handle_connect()

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)

    def found_terminator(self):

        # Start of command
        if self._state is ClientStates.waiting:
            self._ibuffer = []
            self._set_state(ClientStates.header)

        # Received header
        elif self._state is ClientStates.header:
            command_code = decode_int32(self._ibuffer[0:4])
            payload_length = decode_int32(self._ibuffer[4:])
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
            hub_event_id = decode_int32(command.payload[0:4])
            client_event_id = self._event_id_translation[hub_event_id]
            # Here, we could get the tick (payload[4:8]), but we don't
            event_kind = decode_uint32(command.payload[8:12])
            event_payload = command.payload[12:]
            logging.debug((
                'Received icEvent. Hub id: {0}; Client id: {1}; '
                'Event kind: {2}; Payload length: {3}').format(
                    hub_event_id, client_event_id, event_kind, len(event_payload)))

            self._handle_event(client_event_id, event_kind, event_payload)


        elif command.command_code == icSetEventIDTranslation:
            hub_event_id = decode_int32(command.payload[0:4])
            client_event_id = decode_int32(command.payload[4:8])
            if client_event_id >= 0:
                self._event_id_translation[hub_event_id] = client_event_id
            else:
                del self._event_id_translation[hub_event_id]

            logging.debug("Handled icSetEventIDTranslation. Hub's event id: {0}; Client's event id: {1}.".format(
                hub_event_id, client_event_id))

        elif command.command_code == icUniqueClientID:

            self._unique_client_id = decode_uint32(command.payload[0:4])
            self._client_id = decode_uint32(command.payload[4:8])

            logging.debug('Handled icUniqueClientID. Unique client id: {0}; Client id: {1}'.format(
                self._unique_client_id, self._client_id))

        elif command.command_code == icEndSession:
            self.disconnect()

            logging.debug('Handled icEndSession.')

        else:
            pass

    def _handle_event(self, event_id, event_kind, event_payload):
        event = self._event_definitions[event_id]
        event.handle_event(event_kind, event_payload)

    def disconnect(self):
        logging.info('Closing connection to {0}'.format(*self.socket.getpeername()))
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

    def signal_subscribe(self, event_id, event_entry_type, event_name):
        payload = b''.join([
            encode_int32(event_id),
            encode_int32(event_entry_type),
            encode_string(event_name)])

        self._signal_command(Command(command_code=icSubscribe, payload=payload))

    def signal_publish(self, event_id, event_entry_type, event_name):
        payload = b''.join([
            encode_int32(event_id),
            encode_int32(event_entry_type),
            encode_string(event_name)])

        self._signal_command(Command(command_code=icPublish, payload=payload))

    def signal_unsubscribe(self, event_name):
        payload = encode_string(event_name)

        self._signal_command(Command(command_code=icUnsubscribe, payload=payload))

    def signal_unpublish(self, event_name):
        payload = encode_string(event_name)

        self._signal_command(Command(command_code=icUnpublish, payload=payload))

    def signal_normal_event(self, event_id, event_kind, event_payload):
        payload = b''.join([
            encode_int32(event_id),
            encode_int32(0),
            encode_int32(event_kind),
            event_payload])

        self._signal_command(Command(command_code=icEvent, payload=payload))    

    def subscribe(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        event = self._find_or_create_event(event_name)

        event.subscribe()

        return event

    def unsubscribe(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        event = self._try_get_event(event_name)
        if event:
            event.unsubscribe()

        return event


    def publish(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        event = self._find_or_create_event(event_name)

        event.publish()

        return event

    def unpublish(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        event = self._try_get_event(event_name)
        if event:
            event.unpublish()

        return event

    def _try_get_event(self, event_name):
        for key, event in self._event_definitions.items():
            if event.name == event_name:
                return event

        return None

    def _make_new_event_id(self):
        event_id = 0
        while True:
            if not event_id in self._event_definitions:
                return event_id
            else:
                event_id += 1


    def _find_or_create_event(self, event_name):
        event = self._try_get_event(event_name)
        if not event:
            event_id = self._make_new_event_id()
            event = EventDefinition(event_id, event_name, self)
            self._event_definitions[event_id] = event
        
        return event