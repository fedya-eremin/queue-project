import asyncio
import os
from cassandra.cluster import Cluster
from rabbit import channel_pool
import aio_pika

CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', 'localhost').split(',')
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', 9042))
TABLE_NAME = os.getenv('TABLE_NAME')


cluster = Cluster(
    contact_points=CASSANDRA_HOSTS,
    port=CASSANDRA_PORT,
)
session = cluster.connect()
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace 
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
""")
session.set_keyspace("my_keyspace")

session.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id UUID PRIMARY KEY,
        message TEXT
    );
""")
insert_stmt = session.prepare(f"""
    INSERT INTO my_keyspace.{TABLE_NAME} (id, message)
    VALUES (uuid(), ?);
""")

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            session.execute(insert_stmt, (message.body.decode(),))
            print(f"Сообщение сохранено: {message.body.decode()}")
        except Exception as e:
            print(f"Ошибка сохранения: {str(e)}")
            raise

async def main():
    if not TABLE_NAME:
        print("Ошибка: TABLE_NAME не задана")
        return

    async with channel_pool.acquire() as channel:
        queue = await channel.declare_queue(
            "new_queue",
            durable=True
        )
        
        await queue.bind(
            await channel.declare_exchange(
                "new_exchange",
                type=aio_pika.ExchangeType.DIRECT,
                durable=True
            ),
            routing_key="new_queue"
        )
        
        print("Ожидание сообщений...")
        await queue.consume(process_message)

if __name__ == "__main__":
    asyncio.run(main())

