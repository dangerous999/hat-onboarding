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
    def register_event(self, read_type, counter_id, value):
        self._event_client.register([
            common.RegisterEvent(
                event_type = (*self._event_type_prefix,
                              'gateway', read_type, 'counter', counter_id),
                source_timestamp = common.now(),
                payload = common.EventPayload(
                    type=common.EventPayloadType.JSON,
                    data={'value': value}))
        ])

    async def read(self, conn, device_id, data_type, start_address, quantity):
        return await conn.read(device_id = 1,
                               data_type=modbus.DataType.HOLDING_REGISTER,
                               start_address=100,
                               quantity=2)

    async def connect_loop(self):
        while True:
            try:
                self._conn = await modbus.create_tcp_master(modbus.ModbusType.TCP,
                                               tcp.Address(host='localhost',
                                               port=2555))
                await self.read_loop()
            except ConnectionError as e:
                print("ConnError", e)
            except asyncio.CancelledError as e:
                print("CancelledError", e)
                break
            await asyncio.sleep(1)

    async def events_loop(self):
        while True:
            events = await self._event_client.receive()
            for event in events:
                print("device_event", event.event_type)
                if event.event_type[-1] == 'manual_read':
                    result = await self.read(self._conn, None, None, None, None)
                    self.register_event('manual_read_result', '1', result[0])
                    self.register_event('manual_read_result', '2', result[1])

    async def read_loop(self):
        while not self._conn.is_closed:
            try:
                print("read: ", self._name)
                result = await self.read(self._conn, None, None, None, None)
                self.register_event('read_result', '1', result[0])
                self.register_event('read_result', '2', result[1])
            except ConnectionError as e:
                print("read COnnectionError", e)
            except asyncio.CancelledError as e:
                print("read CancelledError", e)
            await asyncio.sleep(3)

    @property
    def async_group(self):
        return self._async_group

# ovo se zove kad se enable-a stvar
def create(config, event_client, event_type_prefix):
    print(config)
    master = ModbusMaster()
    master._event_type_prefix = event_type_prefix
    master._name = config['name']
    master._conn = None
    master._async_group = aio.Group()
    master._event_client = event_client
    master._async_group.spawn(master.connect_loop)
    master._async_group.spawn(master.events_loop)

    return master


