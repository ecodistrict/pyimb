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

        
def decode_header(header):
    return (decode_Int32(header[0:4]), decode_Int32(header[4:]))

def encode_Int32(value):
    return value.to_bytes(4, BYTEORDER, signed=True)

def decode_Int32(value):
    return int.from_bytes(value, BYTEORDER, signed=True)

def encode_UInt32(value):
    return value.to_bytes(4, BYTEORDER, signed=False)

def decode_UInt32(value):
    return int.from_bytes(value, BYTEORDER, signed=False)

def encode_string(value):
    encoded = value.encode(DEFAULT_ENCODING)
    return b''.join([
        encode_Int32(len(encoded)),
        encoded])

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
    def __init__(self, *args, **kwargs):
        self._channels_map = {}
        super(Client, self).__init__(map=self._channels_map, *args, **kwargs)
        
        self._ibuffer = []
        self._state = None
        self._set_state(ClientStates.waiting)
        self._command = None

        self._event_id_translation = {} # hub ID to client ID
        self._event_names = {}
        self._next_event_id = 0

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
    
    def connect(self, host, port, owner_id=None, owner_name=None, federation=None):
        self._federation = federation
        logging.info(
            'Connecting to {0}:{1}. Owner ID: {2}; Owner name: {3}; Federation: {4}.'.format(
                host, port, owner_id, owner_name, federation))
        
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        super(Client, self).connect((host, port))
        t = threading.Thread(target=partial(asyncore.loop, map=self._channels_map))
        t.start()
        
        self._signal_client_info(owner_id, owner_name)

    def handle_connect(self):
        logging.info('Connected to {0}:{1}'.format(*self.socket.getpeername()))
        super(Client, self).handle_connect()

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)

    def _close_socket(self):
        logging.info('Closing connection to {0}'.format(*self.socket.getpeername()))
        self.socket.shutdown(socket.SHUT_RDWR)
        self.close()

    def found_terminator(self):
        if self._state is ClientStates.waiting:
            self._ibuffer = []
            self._set_state(ClientStates.header)

        elif self._state is ClientStates.header:
            command_code, payload_length = decode_header(b''.join(self._ibuffer))
            self._command = Command(command_code=command_code)
            self._ibuffer = []
            if payload_length == 0:
                self._handle_command(self._command)
                self._set_state(ClientStates.waiting)
            else:
                self._set_state(ClientStates.payload)

        elif self._state is ClientStates.payload:
            self._set_state(ClientStates.waiting)
            self._command.payload = b''.join(self._ibuffer)
            self._handle_command(self._command)
    

    def _handle_command(self, command):

        if command.command_code == icEvent:
            hub_event_id = decode_Int32(command.payload[0:4])
            client_event_id = self._event_id_translation[hub_event_id]
            # Here, we could get the tick (payload[4:8]), but we don't
            event_kind = decode_UInt32(command.payload[8:12])
            event_payload = command.payload[12:]
            logging.debug((
                'Received icEvent. Hub id: {0}; Client id: {1}; '
                'Event kind: {2}; Payload length: {3}').format(
                    hub_event_id, client_event_id, event_kind, len(event_payload)))

            self._handle_event(client_event_id, event_kind, event_payload)


        elif command.command_code == icSetEventIDTranslation:
            hub_event_id = decode_Int32(command.payload[0:4])
            client_event_id = decode_Int32(command.payload[4:8])
            if client_event_id >= 0:
                self._event_id_translation[hub_event_id] = client_event_id
            else:
                del self._event_id_translation[hub_event_id]

            logging.debug("Handled icSetEventIDTranslation. Hub's event id: {0}; Client's event id: {1}.".format(
                hub_event_id, client_event_id))

        elif command.command_code == icUniqueClientID:

            self._unique_client_id = decode_UInt32(command.payload[0:4])
            self._client_id = decode_UInt32(command.payload[4:8])

            logging.debug('Handled icUniqueClientID. Unique client id: {0}; Client id: {1}'.format(
                self._unique_client_id, self._client_id))

        elif command.command_code == icEndSession:
            self.end_session()

            logging.debug('Handled icEndSession.')

        else:
            pass

    def _handle_event(self, event_id, event_kind, event_payload):
        raise NotImplementedError()

        
    def end_session(self):
        self._close_socket()

    def _signal_command(self, message):
        parts = [
            MAGIC_BYTES,
            encode_Int32(message.command_code),
            encode_Int32(message.length)]

        if message.payload:
            if len(message.payload) > 0:
                parts.append(message.payload)
                parts.append(END_PAYLOAD_MAGIC_BYTES)

        self.push(b''.join(parts))

    def _signal_client_info(self, owner_id, owner_name):
        payload = b''.join([
            encode_Int32(owner_id),
            encode_string(owner_name)])

        self._signal_command(Command(command_code=icSetClientInfo, payload=payload))

    def _signal_subscribe(self, event_id, event_entry_type, event_name):
        payload = b''.join([
            encode_Int32(event_id),
            encode_Int32(event_entry_type),
            encode_string(event_name)])

        self._signal_command(Command(command_code=icSubscribe, payload=payload))

    def _signal_publish(self, event_id, event_entry_type, event_name):
        payload = b''.join([
            encode_Int32(event_id),
            encode_Int32(event_entry_type),
            encode_string(event_name)])

        self._signal_command(Command(command_code=icPublish, payload=payload))

    def _signal_unsubscribe(self, event_name):
        payload = encode_string(event_name)

        self._signal_command(Command(command_code=icUnsubscribe, payload=payload))

    def _signal_unpublish(self, event_name):
        payload = encode_string(event_name)

        self._signal_command(Command(command_code=icUnpublish, payload=payload))

    def _signal_normal_event(self, event_id, event_kind, event_payload):
        payload = b''.join([
            encode_Int32(event_id),
            encode_Int32(0),
            encode_Int32(event_kind),
            event_payload])

        self._signal_command(Command(command_code=icEvent, payload=payload))

    def _signal_change_object(self, event_id, action, object_id, attribute):
        event_payload = b''.join([
            encode_Int32(action),
            encode_Int32(object_id),
            encode_string(attribute)])

        self._signal_normal_event(event_id, ekChangeObjectEvent, event_payload)

    def subscribe(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        event_id = self._add_or_set_event(event_name)

        event_entry_type = 0
        self._signal_subscribe(event_id, event_entry_type, event_name)

    def unsubscribe(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        # event_id = self._try_get_event_id(event_name)

        self._signal_unsubscribe(event_name)

    def publish(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        event_id = self._add_or_set_event(event_name)

        event_entry_type = 0
        self._signal_publish(event_id, event_entry_type, event_name)

    def unpublish(self, event_name, prefix=True):
        if prefix:
            event_name = self.federation + '.' + event_name

        # event_id = self._try_get_event_id(event_name)

        self._signal_unpublish(event_name)

    def _try_get_event_id(self, event_name):
        event_id = None
        for key in self._event_names:
            if self._event_names[key] == event_name:
                event_id = key
                break

        return event_id

    def _add_or_set_event(self, event_name):
        event_id = self._try_get_event_id(event_name)

        if not event_id:
            event_id = 0
            while True:
                if not event_id in self._event_names:
                    return event_id
                else:
                    event_id += 1
            self._event_names[self._generate_event_id()] = event_name

        return event_id