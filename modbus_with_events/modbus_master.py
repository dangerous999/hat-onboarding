# receieve read requests from event server
# read stuff from modbus device
# publish read results
import sys
from hat.drivers import modbus
from hat.drivers import tcp
from hat.event import eventer_client
from hat import aio
from hat.event import common

def print_event(event):
    print('>'*30)
    print('id' ,event.event_id)
    print('event_type' ,event.event_type)
    print('event_timestamp' ,event.timestamp)
    print('source_timestamp' ,event.source_timestamp)
    print(event.payload.data)
    print('<'*30)

class ModbusMaster:
    def __init__(self):
        self._conn = None

    async def start(self):
        self._conn = await modbus.create_tcp_master(modbus.ModbusType.TCP,
                                                    tcp.Address(host='localhost',
                                                                port=2555))

    async def handle_close(self):
         await self._conn.wait_closed()

    async def read(self, device_id, data_type, start_address, quantity):
        return await self._conn.read(device_id = 1,
                                    data_type=modbus.DataType.HOLDING_REGISTER,
                                    start_address=100,
                                    quantity=2)

async def async_main():
    event_client = await eventer_client.connect(address='tcp+sbs://localhost:23012',
                                                subscriptions=[('modbus', 'read', 'counter', '?')])

    master = ModbusMaster()
    await master.start()

    try:
        result = await master.read(1, modbus.DataType.HOLDING_REGISTER, 104, 1)
        print(result)
    except Exception as e:
        print(e)
    pass

    while not event_client.is_closed:
        events = await event_client.receive()

        for event in events:
            print_event(event)
            payload = event.payload.data
            read_result = await master.read(device_id=payload['device_id'],
                                        data_type=payload['data_type'],
                                        start_address=payload['start_address'],
                                        quantity=payload['quantity'])

            event_client.register([common.RegisterEvent(
                    event_type = ('modbus', 'data', 'counter'),
                    source_timestamp = common.now(),
                    payload = common.EventPayload(
                        type=common.EventPayloadType.JSON,
                        data={'values': [read_result]}))
            ])

    await master.handle_close()

def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())

if __name__ == '__main__':
    sys.exit(main())
