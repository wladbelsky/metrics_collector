from typing import Any, List
from flask import Flask
from database import Database, Client

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
    with Database().get_session() as session:
        count = session.query(Client).count()
        test_metric = Metric('test_metric', count, {'test': 1})
        test_metric2 = Metric('test_metric', 7, {'test': 2})
        return Metric.array_to_str([test_metric, test_metric2])


app.run(host='0.0.0.0', port=5000)
