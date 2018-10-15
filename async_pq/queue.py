from typing import Tuple

import datetime as dt

from asyncpg import Connection

DELETE_LIMIT_SM = 100
DELETE_LIMIT_BIG = 10000


class Queue:
    def __init__(self, name: str, connection: Connection):
        self.name = name
        self._queue_table_name = f'queue_{name}'
        self._requests_table_name = f'queue_{name}_requests'
        self._connection = connection

    async def put(self, *entities: str) -> None:
        """ Insert records (dumped JSONs) into queue """
        await self._connection.executemany(
            f"""
            INSERT INTO {self._queue_table_name} (q_data) 
            VALUES ($1)
            """,
            zip(entities),
        )

    async def pop(self, *, limit: int = 1, with_ack: bool = False) -> Tuple[int, list]:
        """
        Get <limit> records from queue.
        If with_ack == True, then it needs acknowledgement
        """
        request_id = await self._connection.fetchval(
            f"""
            INSERT INTO {self._requests_table_name} (r_status) 
            VALUES('wait') 
            RETURNING r_id
            """
        )
        data = (
            await self._connection.fetch(
                f"""
            UPDATE {self._queue_table_name} 
            SET q_request_id=$1 
            WHERE q_id IN (
              SELECT q_id 
              FROM {self._queue_table_name} 
              WHERE q_request_id IS NULL
              ORDER BY q_id
              LIMIT $2
              FOR UPDATE SKIP LOCKED 
              )
            RETURNING q_data;
            """,
                request_id,
                limit,
            )
            or []
        )
        if not data or not with_ack:
            await self.ack(request_id)
        return request_id, [i[0] for i in data]

    async def ack(self, request_id: int) -> bool:
        """ Acknowledge request """
        if await self._connection.fetchval(
            f"""
            UPDATE {self._requests_table_name} 
            SET r_status='done' 
            WHERE r_id=$1 AND r_status='wait' 
            RETURNING r_id
            """,
            request_id,
        ):
            return True
        return False

    async def unack(self, request_id: int) -> bool:
        """ Delete request """
        if await self._connection.fetchval(
            f"""
            DELETE FROM {self._requests_table_name} 
            WHERE r_id=$1 AND r_status='wait' 
            RETURNING r_id
            """,
            request_id,
        ):
            return True
        return False

    async def return_unacked(self, timeout: int, limit: int=DELETE_LIMIT_SM) -> int:
        """ Delete unacked request (queue entities will be with request_id=NULL) """
        return await self._connection.fetchval(
            f"""
            WITH deleted AS (
              DELETE FROM {self._requests_table_name} 
              WHERE ctid IN (
                SELECT ctid
                FROM {self._requests_table_name}
                WHERE r_status='wait' AND created_at < current_timestamp - $1::interval
                LIMIT $2
              )
              RETURNING * 
            )
            SELECT count(*) FROM deleted
            """,
            dt.timedelta(seconds=timeout),
            limit,
        )

    async def clean_acked_queue(self, limit: int=DELETE_LIMIT_BIG) -> int:
        """ Delete acked queue entities (request will not be deleted) """
        return await self._connection.fetchval(
            f"""
            WITH deleted AS (
                DELETE FROM {self._queue_table_name} 
                WHERE q_request_id in (
                  SELECT r_id FROM {self._requests_table_name} where r_status='done'
                  LIMIT $1
                ) 
                RETURNING * 
            )
            SELECT count(*) FROM deleted
            """,
            limit
        )


class QueueFabric:
    def __init__(self, connection: Connection):
        self._connection = connection

    async def is_exists_queue(self, name: str) -> bool:
        return await self._connection.fetchval(
            f"""
            SELECT EXISTS(SELECT 1 
            FROM information_schema.tables 
            WHERE table_name='queue_{name}' AND table_schema='public');
            """
        )

    async def _new_queue(self, name: str):
        await self._connection.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'queue_request_status') THEN
                    CREATE TYPE queue_request_status as ENUM ('wait', 'done');
                END IF;
            END$$;
            """
        )
        await self._connection.execute(
            f"""
            CREATE TABLE queue_{name}_requests (
              r_id SERIAL PRIMARY KEY,
              r_status queue_request_status,
              created_at timestamptz NOT NULL DEFAULT current_timestamp
            );
            CREATE TABLE queue_{name} (
              q_id BIGSERIAL PRIMARY KEY, 
              q_data JSON,
              q_request_id INT REFERENCES queue_{name}_requests(r_id) ON DELETE SET NULL
            );
            """
        )

    async def find_queue(self, name: str) -> Queue:
        if not await self.is_exists_queue(name):
            await self._new_queue(name)
        return Queue(name, self._connection)
