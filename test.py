import imb
c = imb.Client()
c.imb_connect(imb.TEST_URL,4000,123,'rasmus132','ecodistrict')
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
