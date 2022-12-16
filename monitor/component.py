import sys
import asyncio
from hat import aio
import hat.monitor.client

async def component_main(args):
    print('component running')
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        print('component stopping')

async def async_main():
    client = await hat.monitor.client.connect(
        {'name': 'test component',
         'group': 'example',
         'monitor_address': "tcp+sbs://127.0.0.1:23010"})
    component = hat.monitor.client.Component(client, component_main)
    component.set_ready(True)

    try:
        await component.wait_closed()
    finally:
        await component.async_close()

def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())
if __name__=="__main__":
    sys.exit(main())