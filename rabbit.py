import os
import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractChannel
from aio_pika.pool import Pool


async def get_connection() -> AbstractRobustConnection:
    return await aio_pika.connect_robust(host=os.getenv("RABBIT_HOST"))

connection_pool: Pool[AbstractRobustConnection] = Pool(get_connection, max_size=20)

async def get_channel() -> AbstractChannel:
    async with connection_pool.acquire() as connection:
        return await connection.channel()

channel_pool = Pool(get_channel, max_size=10)

