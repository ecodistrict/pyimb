import imb
import logging
c = imb.Client()
owner_id = 123
owner_name = 'rasmus'
federation = 'ecodistrict'
c.connect(imb.TEST_URL, imb.TEST_PORT, owner_id, owner_name, federation)

c.subscribe('anything')
#input()
c.publish('anything')
#input()
#c.signal_change_object(0,1,2, 'something')
input()

c.unpublish('anything')
#input()
c.unsubscribe('anything')

input()
c.end_session()
