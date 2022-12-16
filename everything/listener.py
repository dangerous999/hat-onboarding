from hat import aio
from hat.event import eventer_client
import sys


async def async_main():
    client = await eventer_client.connect(
        address='tcp+sbs://localhost:23012',
        subscriptions=[('*', )])
    while not client.is_closed:
        events = await client.receive()
        for event in events:
            print_event(event)
    await client.async_close()


def print_event(event):
    # print('>'*80)
    # print('id', event.event_id)
    print('event_type', event.event_type)
    # print('timestamp', event.timestamp)
    # print('source_timestamp', event.source_timestamp)
    # print('payload', event.payload and event.payload.data)
    # print('<'*80)


def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())


if __name__ == '__main__':
    sys.exit(main())
