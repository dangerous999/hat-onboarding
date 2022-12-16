import sys
from hat.drivers import iec104
from hat.drivers import tcp
from hat import aio
from hat.event import eventer_client
from hat.event import common
import asyncio

def print_event(event):
    print('>'*30)
    print('id' ,event.event_id)
    print('event_type' ,event.event_type)
    print('event_timestamp' ,event.timestamp)
    print('source_timestamp' ,event.source_timestamp)
    print(event.payload.data)
    print('<'*30)

async def publish_loop(client):
    event_type = ('modbus', 'read', 'counter', '1')
    payload = common.EventPayload(type=common.EventPayloadType.JSON,
                                data={'device_id': 1,
                                      'data_type': 'HOLDING_REGISTER',
                                      'start_address': 100,
                                      'quantity': 2})
    for i in range(1_000_000):
        client.register([
            common.RegisterEvent(event_type = event_type,
                source_timestamp = common.now(),
                payload = payload)
        ])

        print("published: ", i)

        await asyncio.sleep(1)

async def consume_loop(client):
    while not client.is_closed:
        events = await client.receive()

        for event in events:
            print(event.payload.data)

async def async_main():
    client = await eventer_client.connect(address='tcp+sbs://localhost:23012',
                                          subscriptions=[('modbus', 'data', '*')])

    publish_task = asyncio.create_task(publish_loop(client=client))
    consume_task = asyncio.create_task(consume_loop(client=client))

    await publish_task
    await consume_task
    await client.async_close()

def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())

if __name__ == '__main__':
    sys.exit(main())