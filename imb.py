import sys
import threading
import time
import socket
import asyncore
import asynchat
import logging
from functools import partial
from enum import Enum

PORT = 4000
MAGIC_BYTES =  bytes((0x2F, 0x47, 0x61, 0x71, 0x95, 0xAD, 0xC5, 0xFB))
BYTEORDER = 'big'
HEADER_LENGTH = 8 #bytes

logging.basicConfig(format='[%(levelname)8s] %(message)s', level=logging.DEBUG)

class ClientModes(Enum):
    """docstring for ClientModes"""
    waiting = 0
    header = 1
    payload = 2
        

class Message(object):
    """docstring for Message"""
    def __init__(self, header):
        super(Message, self).__init__()
        self.header = header
        self.payload = None

    @property
    def payload(self):
        return self._payload
    @payload.setter
    def payload(self, value):
        self._payload = value
    
    
    
class Client(asynchat.async_chat):
    """docstring for Client"""
    def __init__(self, *args, **kwargs):
        super(Client, self).__init__(*args, **kwargs)
        self.set_terminator(MAGIC_BYTES)
        self._ibuffer = []
        self._state = ClientModes.waiting
        self._message = None

    def handle_connect(self):
        logging.info('Connected!')
        super(Client, self).handle_connect()

    def collect_incoming_data(self, data):
        self._ibuffer.append(data)
        logging.debug('{0}: appended {1} to buffer'.format(self.socket.getpeername(), str(data)))

    def close(self):
        logging.info('Closing connection to {0}'.format(self.socket.getpeername()))
        super(Client, self).close()

    def found_terminator(self):
        logging.info('Found terminator.')
        if self._state is ClientModes.waiting:
            self._ibuffer = []
            self._state = ClientModes.header
            self.set_terminator(HEADER_LENGTH)
        elif self._state is ClientModes.header:
            self.message = Message(self._ibuffer)
            self._ibuffer = []
            if self.message.length == 0:
                self.set_terminator(MAGIC_BYTES)
                self._state = ClientModes.waiting
                self.handle_message(self.message)
            else:
                self.set_terminator(self.message.length)
                self._state = ClientModes.payload
        elif self.state is ClientModes.payload:
            self.message.payload = self._ibuffer
            self.set_terminator(MAGIC_BYTES)
            self._state = ClientModes.waiting
            self.handle_message(self.message)


        def handle_message(self, msg):
            print('Message:', msg.header, msg.payload)        
        

        # try:
        #     while True:
        #         message = input('> ')
        #         print(message)
        #         self.push_message(message)
        #         if message == 'CLOSE':
        #             logging.info('Stopping client in an orderly manner.')
        #             break
        # except KeyboardInterrupt as e:
        #     logging.info('Forcing client stop.')
        # except BaseException as e:
        #     logging.error('Error while running client.')
        #     logging.debug(e, exc_info=sys.exc_info())
        # finally:
        #     self.close()