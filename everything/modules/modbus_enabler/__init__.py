from hat.event.server import common
from hat import aio
from hat import json
from pathlib import Path

json_schema_id = 'my_stuff://everything/modules/modbus_enabler/schema.yaml#'
json_schema_repo = json.SchemaRepository(
    Path(__file__).parent / 'schema.yaml')


async def create(conf, engine, source):
    module = ModbusEnablerModule()
    module._async_group = aio.Group()

    subscriptions = []
    for device_type in conf['device_types']:
        subscriptions.append(('gateway', '?', device_type,
                              '?', 'gateway', 'running'))
    print(subscriptions)

    module._subscription = common.Subscription(subscriptions)
    return module


class ModbusEnablerModule(common.Module):
    @property
    def async_group(self):
        return self._async_group

    @property
    def subscription(self):
        return self._subscription

    async def process(self, source, event):
        device_type = event.event_type[3]

        yield common.RegisterEvent(
                event_type=('gateway', 'dino_gw', 'my_modbus_device',
                            device_type, 'system', 'enable'),
                source_timestamp=common.now(),
                payload=common.EventPayload(
                    type=common.EventPayloadType.JSON,
                    data=True)
                )