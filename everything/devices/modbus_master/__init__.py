import asyncio
import logging
import hat.gateway.common
from pathlib import Path
from hat import aio
from hat import json
from hat.drivers import modbus
from hat.drivers import tcp
from hat.drivers.modbus import common as mc
from hat.event import common

device_type = 'my_modbus_device'
json_schema_id = 'my_stuff://everything/devices/modbus_master/schema.yaml#'
json_schema_repo = json.SchemaRepository(
    Path(__file__).parent / 'schema.yaml')

mlog = logging.getLogger(__name__)


class ModbusMaster(hat.gateway.common.Device):
    def register_event(self, read_type, counter_id, value):
        self._event_client.register([
            common.RegisterEvent(
                event_type=(*self._event_type_prefix,
                            'gateway', 'modbus_payload'),
                source_timestamp=common.now(),
                payload=common.EventPayload(
                    type=common.EventPayloadType.JSON,
                    data={'value': value,
                          'read_type': read_type,
                          'counter': counter_id,
                          }))
        ])

    async def read(self, device_id, data_type, start_address, quantity):
        return await self._conn.read(device_id=device_id,
                                     data_type=data_type,
                                     start_address=start_address,
                                     quantity=quantity)

    async def connect_loop(self):
        while True:
            try:
                self._conn = await modbus.create_tcp_master(
                                        modbus.ModbusType.TCP,
                                        tcp.Address(host=self._address,
                                                    port=self._port))
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
                try:
                    if event.event_type[-1] == 'manual_read':
                        data = event.payload[1]
                        counter_id = data['counter_id']
                        start_address = data['start_address']
                        quantity = data['quantity']
                        data_type = mc.DataType[data['data_type']]

                        result = await self.read(counter_id,
                                                 data_type,
                                                 start_address,
                                                 quantity)

                        self.register_event('manual_read_result',
                                            str(counter_id),
                                            result)
                except Exception as e:
                    mlog.error("manual read error", exc_info=e)

    async def read_loop(self):
        while not self._conn.is_closed:
            try:
                for data_point in self._data:
                    data_type = mc.DataType[data_point['data_type']]
                    result = await self.read(
                                        data_point['counter_id'],
                                        data_type,
                                        data_point['start_address'],
                                        data_point['quantity'])

                    self.register_event('read_result',
                                        str(data_point['counter_id']),
                                        result)
            except ConnectionError as e:
                print("read COnnectionError", e)
            except asyncio.CancelledError as e:
                print("read CancelledError", e)
            except Exception as e:
                print("error", e)

            await asyncio.sleep(self._interval)

    @property
    def async_group(self):
        return self._async_group


def create(config, event_client, event_type_prefix):
    print(config)
    print(event_type_prefix)
    master = ModbusMaster()
    master._event_type_prefix = event_type_prefix
    master._name = config['name']
    master._conn = None
    master._address = config['address']
    master._port = config['port']
    master._interval = config['interval']
    master._data = []

    for x in config['data']:
        master._data.append(x)

    master._async_group = aio.Group()
    master._event_client = event_client
    master._async_group.spawn(master.connect_loop)
    master._async_group.spawn(master.events_loop)

    return master