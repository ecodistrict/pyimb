import imb
import logging
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

input()
anything.signal_change_object(1,2, 'something')
input()


c.unpublish('anything')
#input()
c.unsubscribe('anything')

input()
c.disconnect()
