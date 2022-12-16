from hat import aio
from hat.drivers import modbus
from hat.drivers import tcp
from hat.event import common
import hat.gateway.common
import asyncio

device_type = 'my_modbus_device'
json_schema_id = None
json_schema_repo = None

class ModbusMaster(hat.gateway.common.Device):
    def __init__(self, event_client):
        self._conn = None
        self._async_group = aio.Group()
        self._event_client = event_client

    def start(self):
        self._async_group.spawn(self.connect_loop)

    async def handle_close(self):
         await self._conn.wait_closed()

    async def read(self, conn, device_id, data_type, start_address, quantity):
        return await conn.read(device_id = 1,
                               data_type=modbus.DataType.HOLDING_REGISTER,
                               start_address=100,
                               quantity=2)

    async def connect_loop(self):
        while True:
            try:
                conn = await modbus.create_tcp_master(modbus.ModbusType.TCP,
                                               tcp.Address(host='localhost',
                                               port=2555))
                await self.read_loop(conn)
            except ConnectionError as e:
                print("ConnError", e)
            except asyncio.CancelledError as e:
                print("CancelledError", e)
            await asyncio.sleep(1)

    async def read_loop(self, conn):
        while not conn.is_closed:
            try:
                result = await self.read(conn, None, None, None, None)
                self._event_client.register([common.RegisterEvent(
                        event_type = ('modbus', 'data', 'counter'),
                        source_timestamp = common.now(),
                        payload = common.EventPayload(
                            type=common.EventPayloadType.JSON,
                            data={'values': [result]}))
                ])
            except ConnectionError as e:
                print("read COnnectionError", e)
            except asyncio.CancelledError as e:
                print("read CancelledError", e)
            await asyncio.sleep(3)

    @property
    def async_group(self):
        return self._async_group

# ovo se zove kad se enable-a stvar
def create(config, event_client, event_type):
    print(config)
    master = ModbusMaster(event_client)
    master.start()
    return master


