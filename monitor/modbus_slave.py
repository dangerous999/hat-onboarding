import sys
from hat.drivers import modbus
from hat.drivers import tcp
from hat import aio

counter = 2

async def read_cb(slave, device_id, data_type, start_address, quantity):
    global counter
    counter = counter + 1
    return [counter, counter + 1]

async def async_main():
    while True:
        address = tcp.Address(host='localhost', port=2555)

        queue = aio.Queue()
        slave = await modbus.create_tcp_server(modbus.ModbusType.TCP,
                                            address,
                                            slave_cb=queue.put_nowait,
                                            read_cb=read_cb)

        connection = await queue.get()

        await connection.wait_closing()

        await connection.async_close()
        await slave.async_close()
        await slave.wait_closed()

def main():
    aio.init_asyncio()
    aio.run_asyncio(async_main())

if __name__ == '__main__':
    sys.exit(main())