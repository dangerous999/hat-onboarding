import asyncio
from hat import aio
from hat.event import common
import hat.gui.common
from hat.util import first
from pprint import pprint
import random

json_schema_id = None
json_schema_repo = None

def print_event(event):
    print('>'*80)
    print('id', event.event_id)
    print('event_type', event.event_type)
    print('timestamp', event.timestamp)
    print('source_timestamp', event.source_timestamp)
    print('payload', event.payload and event.payload.data)
    print('<'*80)

def create_subscription(data):
    print("data", data)
    subscriptions = []
    for subscription in data['subscriptions']:
        subscriptions.append(tuple(subscription))

    return common.Subscription(subscriptions)

def create_adapter(data, event_client):
    print("create_adapter", data)
    adapter = MyAdapter()
    adapter._async_group = aio.Group()
    adapter._event_client = event_client
    adapter._async_group.spawn(adapter.recieve_loop)
    adapter._my_session = None
    adapter._mapping = data['items']
    return adapter

class MyAdapter(hat.gui.common.Adapter):
    async def recieve_loop(self):
        print("recv loop")
        while not self._event_client.is_closed:
            events = await self._event_client.receive()
            for event in events:
                # print("adapter event", event.event_type)
                if self._my_session:
                    self._my_session.update_state(event)

        await self._event_client.async_close()

    @property
    def async_group(self):
        return self._async_group

    async def create_session(self, juggler_client):
        self._my_session = MyAdapterSession()
        self._my_session._async_group = self._async_group.create_subgroup()
        self._my_session._juggler_client = juggler_client
        self._my_session._async_group.spawn(self._my_session.juggler_listen)
        self._my_session._mapping = self._mapping
        self._my_session._state = {}
        self._my_session._event_client = self._event_client

        return self._my_session

class MyAdapterSession(aio.Resource):
    @property
    def async_group(self):
        return self._async_group

    async def juggler_listen(self):
        try:
            while not self._juggler_client.is_closed:
                msg = await self._juggler_client.receive()
                # print('recv juggler message:', msg)
                if msg['type'] != 'manual_read':
                    continue
                # create event that will read a specific device
                event_type = ('gateway', 'dino_gw', 'my_modbus_device',
                            msg['ID'], 'system', 'manual_read')
                self._event_client.register([
                    common.RegisterEvent(
                        event_type = event_type,
                        source_timestamp = common.now(),
                        payload = common.EventPayload(
                            type=common.EventPayloadType.JSON,
                            data={'value': msg['ID']}))
                ])
        except ConnectionError:
            print('server closed')
        except asyncio.CancelledError:
            print('client closed')


    # TODO State
    def update_state(self, event):
        event_type = list(event.event_type)

        counter = event_type[-1]

        key = first(self._mapping,
                    lambda m: m['event_type'][-1] == counter)

        self._state[key['key']] = event.payload.data['value']

        print("update state", self._state)
        # print("update state", {key['key']: event.payload.data['value']})
        # self._juggler_client.set_local_data(
        #     {key['key']: event.payload.data['value']})
        self._juggler_client.set_local_data(self._state)
