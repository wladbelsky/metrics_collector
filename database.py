import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import CHAR, Column, Date, DateTime, Float, ForeignKey, String, Text, Time, text
from sqlalchemy.dialects.mysql import INTEGER, SMALLINT, TINYINT, TINYTEXT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool

Base = sqlalchemy.ext.declarative.declarative_base()
metadata = Base.metadata


class Database(object):
    def __init__(self, connection: dict) -> None:
        self.engine = create_async_engine(self.prepare_connection_string(
            connection
        ), poolclass=NullPool, echo=False)
        self.host = connection.get('host')
        self.connection = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            await self.disconnect()

    async def connect(self):
        if not self.connection:
            self.connection = await self.engine.connect()
            self.metadata = metadata

    async def disconnect(self):
        if self.connection:
            await self.connection.close()
            self.connection = None

    def get_session(self) -> AsyncSession:
        return sqlalchemy.orm.sessionmaker(bind=self.engine, class_=AsyncSession)()

    def insert_if_not_exsits(self, table, **data):
        with self.get_session() as session:
            result = session.query(table).filter_by(**data).first()
            if not result:
                session.merge(table(**data))
                session.commit()
            return session.query(table).filter_by(**data).first() if not result else result

    @staticmethod
    def prepare_connection_string(config: dict) -> str:
        user = config.get('user') or ''
        password = config.get('password') or ''
        host = config.get('host')
        port = config.get('port') or ''
        database = config.get('database') or ''
        engine = config.get('engine')
        return f'{engine}://{user}{":" if user and password else ""}{password}{"@" if user and password else ""}{host}{":" if port else ""}{port}{"/" if database else ""}{database}'
