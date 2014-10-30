import imb
import logging
import os.path
owner_id = 123
owner_name = 'rasmus'
federation = 'ecodistrict'

c = imb.Client(imb.TEST_URL, imb.TEST_PORT, owner_id, owner_name, federation)
#print(c.unique_client_id)
anything = c.subscribe('anything')
#input()
anything.publish()
#input()


anything.add_handler(imb.ekChangeObjectEvent, 
    lambda action, object_id, short_event_name, attr_name: print('ChangeObjectEvent', action, object_id, short_event_name, attr_name))

def create_stream(event, stream_name):
    logging.debug(stream_name)
    path = os.path.join('test_dir', stream_name)
    stream = open(path, 'wb+')
    logging.debug('Creating file stream at {0}.'.format(path))
    return stream

anything.create_stream_callback = create_stream
anything.end_stream_callback = lambda event, stream: logging.debug('End stream callback called!')

# input()
# anything.signal_change_object(1,2, 'something')
input()

anything.signal_stream('my old file', open('README.md', 'rb'))

c.unpublish('anything')
#input()
c.unsubscribe('anything')

input()
c.disconnect()
