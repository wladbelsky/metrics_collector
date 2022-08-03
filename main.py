import asyncio
import logging
import time

from async_timeout import timeout
from dataclasses import dataclass, field
from typing import Any
from typing import List, Union, Coroutine, Iterable

import sqlalchemy.sql
import sqlalchemy.sql
from flask import Flask

import util
from connections import connects, timeout_time
from database import Database

app = Flask(__name__)
app.logger.setLevel(logging.INFO)


@dataclass
class Metric:
    name: str
    query: Union[str, Coroutine, sqlalchemy.sql.Selectable]
    tags: dict = field(default_factory=dict)
    value: Any = None

    def __str__(self):
        tags = ', '.join([f'{k}="{v}"' for k, v in self.tags.items()])
        if tags:
            tags = f'{{{tags}}}'
        return f"{self.name}{tags} {str(self.value)}"

    @classmethod
    def array_to_str(cls, array: Iterable['Metric']) -> str:
        return "\n".join([str(metric) for metric in array if isinstance(metric, cls)])


def run_all_in_database(db: Database, metrics: List[Metric]) -> List[Coroutine]:
    async def run_query(metric: Metric) -> Metric:
        async with timeout(timeout_time):
            try:
                start = time.perf_counter()
                metric.tags['host'] = db.host
                if isinstance(metric.query, (str, sqlalchemy.sql.Selectable)):
                    metric.value = (await db.get_session().execute(metric.query)).one()[0]
                else:
                    metric.value = await metric.query
                metric.execution_time = time.perf_counter() - start
                app.logger.info(f'{str(metric)} execution time: {metric.execution_time}')
                return metric
            except Exception as e:
                app.logger.error(f'{str(metric)} exception occurred: {e}')
                app.logger.exception(e)
                raise
    return [run_query(metric) for metric in metrics]

@app.route('/metrics')
async def main():
    queries = []
    connections = [Database(connection) for connection in connects]
    for connection in connections:
        await connection.connect()
        queries.extend(run_all_in_database(connection,
                                           [
                                               Metric('EMK_cases_status_2', query=EMK_Send_st2(connection)),
                                               Metric('EMK_cases_status_3', query=EMK_Send_st3(connection)),
                                              # Metric('Test', query='select rand()',
                                              #        tags={'host': connection.host})
                                           ]))
    results = await asyncio.gather(*queries, return_exceptions=True)
    for connection in connections:
        await connection.disconnect()
    exceptions = filter(lambda x: isinstance(x, Exception), results)
    results = filter(lambda x: isinstance(x, Metric), results)
    for exception in exceptions:
        app.logger.exception(exception)
        app.logger.error(exception.__class__.__name__)
    return Metric.array_to_str(results)


async def EMK_Send_st3(db: Database) -> int:
    async with db.get_session() as session:
        count_status2 = (await session.execute(util.IEMK_STATUS_2)).one()
        return count_status2[0]


async def EMK_Send_st2(db: Database) -> int:
    async with db.get_session() as session:
        count_status3 = (await session.execute(util.IEMK_STATUS_3)).one()
        return count_status3[0]


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9101)
