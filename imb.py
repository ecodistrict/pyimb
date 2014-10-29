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

logging.basicConfig(format='[%(levelname)8s] %(message)s', level=logging.DEBUG)

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
    def __init__(self, host, port, *args, **kwargs):
        self.map = {}
        super(Client, self).__init__(map=self.map, *args, **kwargs)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        self.set_terminator(MAGIC_BYTES)
        self._ibuffer = []
        self._state = ClientModes.waiting
        self._message = None

        logging.info('Connecting to {0}:{1}'.format(host, port))
        t = threading.Thread(target=partial(asyncore.loop, map=self.map))
        t.start()


    def handle_connect(self):
        logging.info('Connected!')
        super(Client, self).handle_connect()

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)

    def close(self):
        logging.info('Closing connection to {0}'.format(self.socket.getpeername()))
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

    def send_message(self, message):
        parts = [
            MAGIC_BYTES,
            encode_Int32(message.command),
            encode_Int32(message.length)]

        if message.payload:
            if len(message.payload) > 0:
                parts.append(message.payload)
                parts.append(END_PAYLOAD_MAGIC_BYTES)

        self.push(b''.join(parts))