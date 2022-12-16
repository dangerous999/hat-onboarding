from hat import aio
from hat.event import eventer_client
from hat.event import common
from hat import util
import sys

async def async_main():
    client = await eventer_client.connect(
        address='tcp+sbs://localhost:23012',
        subscriptions=[('gateway', 'dino_gw', '*')])

    await client.register_with_response([
        common.RegisterEvent(
            event_type=('gateway', 'dino_gw', 'my_modbus_device', 'my_device', 'system',
                        'enable'),
            source_timestamp=common.now(),
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=True)),
        common.RegisterEvent(
            event_type=('gateway', 'dino_gw', 'my_modbus_device', 'my_device_2', 'system',
                        'enable'),
            source_timestamp=common.now(),
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data=True))
        ])

    # while True:
    #     events = await client.receive()

    #     for event in events:
    #         print(event.event_type)

    #     res = util.first(
    #         events,
    #         lambda e: e.event_type == ('gateway', 'dino_gw',
    #                                 'modbus_master', 'device_modbus', 'gateway',
    #                                 'remote_device', '1', 'status'))

    #     if res is not None:
    #         break

    # await client.register_with_response([
    #     common.RegisterEvent(
    #         event_type=('gateway', 'dino_gw', 'modbus_master', 'device_modbus', 'system',
    #                     'remote_device', '1', 'enable'),
    #         source_timestamp=common.now(),
    #         payload=common.EventPayload(
    #             type=common.EventPayloadType.JSON,
    #             data=True))])
    await client.async_close()


def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())


if __name__ == '__main__':
    sys.exit(main())
