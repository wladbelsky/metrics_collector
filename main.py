import asyncio

import sqlalchemy.sql
from dataclasses import dataclass
from typing import List, Union, Coroutine, Iterable
from flask import Flask

from connections import connects
from database import Database, Client
import util

app = Flask(__name__)


@dataclass
class Metric:
    name: str
    tags: dict
    query: Union[str, Coroutine, sqlalchemy.sql.Selectable]
    value: str = None

    def __str__(self):
        tags = ', '.join([f'{k}="{v}"' for k, v in self.tags.items()])
        if tags:
            tags = f'{{{tags}}}'
        return f"{self.name}{tags} {self.value}"

    @classmethod
    def array_to_str(cls, array: Iterable['Metric']):
        return "\n".join([str(metric) for metric in array if isinstance(metric, cls)])


def run_all_in_database(db: Database, metrics: List[Metric]) -> List[Coroutine]:
    async def run_query(metric: Metric):
        if isinstance(metric.query, (str, sqlalchemy.sql.Selectable)):
            metric.value = (await db.get_session().execute(metric.query)).one()[0]
        else:
            metric.value = await metric.query
        return metric
    return [run_query(metric) for metric in metrics]

@app.route('/metrics')
async def main():
    queries = []
    connections = [Database(connection) for connection in connects]
    for connection in connections:
        await connection.connect()
        queries.extend(run_all_in_database(connection,
                                           [
                                               Metric('EMK_cases_status_2', query=EMK_Send_st2(connection),
                                                      tags={'host': connection.host}),
                                               Metric('EMK_cases_status_3', query=EMK_Send_st3(connection),
                                                      tags={'host': connection.host}),
                                               Metric('Test', query='select rand()',
                                                      tags={'host': connection.host})
                                           ]))
    results = await asyncio.gather(*queries, return_exceptions=True)
    for connection in connections:
        await connection.disconnect()
    exceptions = filter(lambda x: isinstance(x, Exception), results)
    results = filter(lambda x: isinstance(x, Metric), results)
    for exception in exceptions:
        app.logger.exception(exception)
    return Metric.array_to_str(results)


async def EMK_Send_st3(db):
    async with db.get_session() as session:
        count_status2 = (await session.execute(util.IEMK_STATUS_2)).one()
        return count_status2[0]


async def EMK_Send_st2(db):
    async with db.get_session() as session:
        count_status3 = (await session.execute(util.IEMK_STATUS_3)).one()
        return count_status3[0]


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9101)
