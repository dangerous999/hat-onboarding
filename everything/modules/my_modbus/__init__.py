from hat.event.server import common
from hat import aio
from hat import json
from pathlib import Path

json_schema_id = 'my_stuff://everything/modules/my_modbus/schema.yaml#'
json_schema_repo = json.SchemaRepository(
    Path(__file__).parent / 'schema.yaml')

async def create(conf, engine, source):
    module = MyModbusModule()
    module._async_group = aio.Group()
    module._subscription = common.Subscription([tuple(conf['subscription'])])
    print(tuple(conf['subscription']))
    # module._subscription = common.Subscription([('*')])
    return module

class MyModbusModule(common.Module):
    @property
    def async_group(self):
        return self._async_group

    @property
    def subscription(self):
        return self._subscription

    async def process(self, source, event):
        data = event.payload[1]
        value = data['value']
        read_type = data['read_type']
        device_id = event.event_type[3]
        counter_id = data['counter']
        event_type=('modbus_module', device_id,
                    read_type, 'counter', counter_id)

        yield common.RegisterEvent(
            event_type=event_type,
            source_timestamp=event.source_timestamp,
            payload=common.EventPayload(
                type=common.EventPayloadType.JSON,
                data={'value': value}))