import time

import pytest
import docker
import asyncpg
from async_pq import QueueFabric, Queue

POSTGRES_PASSWORD = 'pass'
POSTGRES_USER = 'user'
POSTGRES_DB = 'test_db'

pytestmark = pytest.mark.asyncio


class TestPq:
    @pytest.fixture
    def run_database(self):
        client = docker.from_env()
        for c in client.containers.list(all=True):
            if c.name == 'pq_test_db':
                return None
        client.images.pull('postgres:9.6')
        postgres_container = client.containers.create(
            image='postgres:9.6',
            ports={5432: 5432},
            environment={
                'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
                'POSTGRES_USER': POSTGRES_USER,
                'POSTGRES_DB': POSTGRES_DB,
            },
            name='pq_test_db',
        )
        postgres_container.start()
        time.sleep(5)  # delay for container initialization

    @pytest.fixture
    async def pg_connection(self, run_database) -> asyncpg.Connection:
        con = await asyncpg.connect(
            host='127.0.0.1',
            port=5432,
            user=POSTGRES_USER,
            database=POSTGRES_DB,
            password=POSTGRES_PASSWORD,
        )
        await con.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            """
        )
        return con

    async def test_fabric(self, pg_connection):
        queue = await QueueFabric(pg_connection).find_queue('items')
        assert isinstance(queue, Queue)

    @pytest.fixture
    async def new_queue(self, pg_connection):
        return await QueueFabric(pg_connection).find_queue('items')

    @pytest.fixture
    async def put_and_pop(self, new_queue: Queue):
        await new_queue.put('"first"', '"second"', '"third"', '"forth"')
        return await new_queue.pop(limit=2, with_ack=False)

    async def test_put_pop(self, put_and_pop):
        request_id, data = put_and_pop
        assert data == ['"first"', '"second"']

    @pytest.fixture
    async def pop_with_ack(self, new_queue, put_and_pop):
        request_id, _ = put_and_pop
        await new_queue.ack(request_id)

    async def test_ack(self, new_queue, pop_with_ack, pg_connection):
        all_queue = await pg_connection.fetch(
            f"""
            SELECT * from {new_queue._queue_table_name} ORDER BY q_id
            """
        )
        assert [list(r) for r in all_queue] == [
            [1, '"first"', 1],
            [2, '"second"', 1],
            [3, '"third"', None],
            [4, '"forth"', None],
        ]
        all_requests = await pg_connection.fetch(
            f"""
            SELECT * from {new_queue._requests_table_name} ORDER BY r_id
            """
        )
        assert [list(r)[:2] for r in all_requests] == [
            [1, 'done']
        ]
