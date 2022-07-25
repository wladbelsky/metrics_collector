import asyncio
from typing import Any, List
from flask import Flask
from database import Database, Client
import util

app = Flask(__name__)


class Metric:
    def __init__(self, name: str, value: Any, tags: dict):
        self.name = name
        self.value = value
        self.tags = tags

    def __str__(self):
        tags = ', '.join([f'{k}="{v}"' for k, v in self.tags.items()])
        if tags:
            tags = f'{{{tags}}}'
        return f"{self.name}{tags} {self.value}"

    @classmethod
    def array_to_str(cls, array: List['Metric']):
        return "\n".join([str(metric) for metric in array])


@app.route('/metrics')
async def main():
    await Database.get_instance()
    coros = [EMK_Send_st2(), EMK_Send_st3()]
    results = await asyncio.gather(*coros)
    return Metric.array_to_str(results)


async def EMK_Send_st3():
    async with await Database.get_class_session() as session:
        count_status2 = (await session.execute(util.IEMK_STATUS_2)).one()
        return Metric('EMK_cases_status_2', count_status2[0], {'status2': 1})


async def EMK_Send_st2():
    async with await Database.get_class_session() as session:
        count_status3 = (await session.execute(util.IEMK_STATUS_3)).one()
        return Metric('EMK_cases_status_3', count_status3[0], {'status3': 1})

app.run(host='0.0.0.0', port=5000)
