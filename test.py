import imb
c = imb.Client()
owner_id = 123
owner_name = 'rasmus'
federation = 'ecodistrict'
c.imb_connect(imb.TEST_URL, imb.TEST_PORT, owner_id, owner_name, federation)
c.signal_subscribe(0, 0, 'anything')
#input()
c.signal_publish(0,0, 'anything')
#input()
c.signal_change_object(0,1,2, 'something')
#input()
c.signal_unpublish('anything')
#input()
c.signal_unsubscribe('anything')

input()
c.close()
