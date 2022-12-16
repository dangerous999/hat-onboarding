from hat import aio
from hat import juggler
from pprint import pprint
import asyncio
import sys

async def notify_cb(client, x, data):
    print('notify', client, x, data)

async def send_messages(juggler_client, group):
        msg = {
            'type': 'adapter',
            'name': 'my_adapter',
            'data':
                {
                    'type': 'manual_read',
                    'ID': 'my_device'
                }
        }

        group.spawn(juggler_client.send, msg)

        msg = {
            'type': 'adapter',
            'name': 'my_adapter',
            'data':
                {
                    'type': 'manual_read',
                    'ID': 'my_device_2'
                }
        }

        group.spawn(juggler_client.send, msg)

async def juggler_send_loop(juggler_client):
    group = aio.Group()
    while not juggler_client.is_closed:
        await send_messages(juggler_client, group)
        await asyncio.sleep(5)

async def async_main():
    client = await juggler.connect('ws://localhost:23023/ws')

    group = aio.Group()

    print('initial', client.remote_data)

    def state_change():
        print("state change: ", client.remote_data)

    client.register_change_cb(state_change)

    await client.send({'type': 'login',
                       'name': 'user1',
                       'password': 'pass1'})

    group.spawn(juggler_send_loop, client)

    try:
        while not client.is_closed:
            print('receiving')
            msg = await client.receive()
            print('msg:', msg)
    except ConnectionError:
        print('server closed')
    except asyncio.CancelledError:
        print('client closed')

def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())

if __name__ == '__main__':
    sys.exit(main())
