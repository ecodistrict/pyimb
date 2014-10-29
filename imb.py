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

class ClientModes(Enum):
    """docstring for ClientModes"""
    waiting = 0
    header = 1
    payload = 2

        
def decode_header(header):
    return (decode_Int32(header[0:4]), decode_Int32(header[4:]))

def encode_Int32(value):
    return value.to_bytes(4, BYTEORDER, signed=True)

def decode_Int32(value):
    return int.from_bytes(value, BYTEORDER, signed=True)

def encode_string(value):
    encoded = value.encode(DEFAULT_ENCODING)
    return b''.join([
        encode_Int32(len(encoded)),
        encoded])

class Message(object):
    """docstring for Message"""
    def __init__(self, command=None, payload=None):
        super(Message, self).__init__()
        self.command = command
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
        self.map = {}
        super(Client, self).__init__(map=self.map, *args, **kwargs)
        
        self.set_terminator(MAGIC_BYTES)
        self._ibuffer = []
        self._state = ClientModes.waiting
        self._message = None

        self._event_id_translation = {}



    @property
    def federation(self):
        return self._federation
    

    def imb_connect(self, host, port, owner_id=None, owner_name=None, federation=None):
        self._federation = federation
        logging.info(
            'Connecting to {0}:{1}. Owner ID: {2}; Owner name: {3}; Federation: {4}.'.format(
                host, port, owner_id, owner_name, federation))
        
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        t = threading.Thread(target=partial(asyncore.loop, map=self.map))
        t.start()
        
        self.signal_client_info(owner_id, owner_name)


    def handle_connect(self):
        logging.info('Connected!')
        super(Client, self).handle_connect()

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)

    def close(self):
        logging.info('Closing connection to {0}'.format(self.socket.getpeername()))
        self.socket.shutdown(socket.SHUT_RDWR)
        super(Client, self).close()

    def found_terminator(self):
        logging.debug('Found terminator in mode {0}.'.format(self._state))
        if self._state is ClientModes.waiting:
            self._ibuffer = []
            self._state = ClientModes.header
            self.set_terminator(HEADER_LENGTH)
        elif self._state is ClientModes.header:
            command, payload_length = decode_header(b''.join(self._ibuffer))
            logging.debug(
                'Expecting command {0} with payload length {1}.'.format(command, payload_length))
            self.message = Message(command=command)
            self._ibuffer = []
            if payload_length == 0:
                self.set_terminator(MAGIC_BYTES)
                self._state = ClientModes.waiting
                self.handle_message(self.message)
            else:
                self.set_terminator(END_PAYLOAD_MAGIC_BYTES)
                self._state = ClientModes.payload
        elif self._state is ClientModes.payload:
            self.message.payload = b''.join(self._ibuffer)
            self.set_terminator(MAGIC_BYTES)
            self._state = ClientModes.waiting
            self.handle_message(self.message)

    def handle_message(self, message):
        logging.debug('Received message: {0} (length {1}): "{2}"'.format(
            message.command, message.length, message.payload))

    def signal_message(self, message):
        parts = [
            MAGIC_BYTES,
            encode_Int32(message.command),
            encode_Int32(message.length)]

        if message.payload:
            if len(message.payload) > 0:
                parts.append(message.payload)
                parts.append(END_PAYLOAD_MAGIC_BYTES)

        self.push(b''.join(parts))

    def signal_client_info(self, owner_id, owner_name):
        payload = b''.join([
            encode_Int32(owner_id),
            encode_string(owner_name)])

        logging.debug('Signalling client info. Payload: {0}'.format(payload))

        self.signal_message(Message(command=icSetClientInfo, payload=payload))

    def signal_subscribe(self, event_id, event_entry_type, event_name):
        payload = b''.join([
            encode_Int32(event_id),
            encode_Int32(event_entry_type),
            encode_string(event_name)])

        self.signal_message(Message(command=icSubscribe, payload=payload))

    def signal_publish(self, event_id, event_entry_type, event_name):
        payload = b''.join([
            encode_Int32(event_id),
            encode_Int32(event_entry_type),
            encode_string(event_name)])

        self.signal_message(Message(command=icPublish, payload=payload))

    def signal_unsubscribe(self, event_name):
        payload = encode_string(event_name)

        self.signal_message(Message(command=icUnsubscribe, payload=payload))

    def signal_unpublish(self, event_name):
        payload = encode_string(event_name)

        self.signal_message(Message(command=icUnpublish, payload=payload))

    def signal_normal_event(self, event_id, event_kind, event_payload):
        payload = b''.join([
            encode_Int32(event_id),
            encode_Int32(0),
            encode_Int32(event_kind),
            event_payload])

        self.signal_message(Message(command=icEvent, payload=payload))

    def signal_change_object(self, event_id, action, object_id, attribute):
        event_payload = b''.join([
            encode_Int32(action),
            encode_Int32(object_id),
            encode_string(attribute)])

        self.signal_normal_event(event_id, ekChangeObjectEvent, event_payload)