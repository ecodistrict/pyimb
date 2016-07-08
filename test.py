import imb
import logging
import msvcrt

owner_id = 124
owner_name = 'lotos-euros'
federation = 'US_RT'

c = imb.Client('web-ecodistrict.tno.nl', 4000, owner_id, owner_name, federation)
print(c.unique_client_id)

anything = c.subscribe('anything3')
anything.publish()

print('waiting before signal change object')
msvcrt.getch()

anything.signal_change_object(1,2, 'something')

print('waiting after signal change object')
msvcrt.getch()

#anything.signal_stream('my old file', open('README.md', 'rb'))

anything.unpublish()
anything.unsubscribe()

print('waiting after unsub/unpub')
msvcrt.getch()


c.disconnect()
