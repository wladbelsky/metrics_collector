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
        ), poolclass=NullPool, echo=True)
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


class Client(Base):
    __tablename__ = 'Client'

    id = Column(INTEGER(11), primary_key=True)
    createDatetime = Column(DateTime, nullable=False, comment='Дата создания записи')
    createPerson_id = Column(ForeignKey('Person.id'), comment='Автор записи {Person}')
    modifyDatetime = Column(DateTime, nullable=False, comment='Дата изменения записи')
    modifyPerson_id = Column(ForeignKey('Person.id'), comment='Автор изменения записи {Person}')
    attendingPerson_id = Column(ForeignKey('Person.id', ondelete='SET NULL', onupdate='CASCADE'))
    deleted = Column(TINYINT(1), nullable=False, server_default=text("0"), comment='Отметка удаления записи')
    lastName = Column(String(30), nullable=False, comment='Фамилия')
    firstName = Column(String(30), nullable=False, comment='Имя')
    patrName = Column(String(30), nullable=False, comment='Отчество')
    birthDate = Column(Date, nullable=False, comment='Дата рождения')
    birthTime = Column(Time, nullable=False, comment='Время рождения')
    sex = Column(TINYINT(4), nullable=False, comment='Пол (0-неопределено, 1-М, 2-Ж)')
    SNILS = Column(CHAR(11), nullable=False, comment='СНИЛС')
    bloodType_id = Column(ForeignKey('rbBloodType.id', ondelete='SET NULL'), comment='Группа крови{rbBloodType}')
    bloodDate = Column(Date, comment='Дата установления группы крови')
    bloodNotes = Column(TINYTEXT, nullable=False, comment='Примечания к группе крови')
    growth = Column(String(16), nullable=False, comment='Рост при рождении')
    weight = Column(String(16), nullable=False, comment='Вес при рождении')
    embryonalPeriodWeek = Column(String(16), nullable=False, comment='Неделя эмбрионального периода(в которую рожден пациент)')
    birthPlace = Column(String(120), nullable=False, server_default=text("''"), comment='Место рождения')
    chronicalMKB = Column(String(8), nullable=False, comment='Код хронического диагноза по МКБ')
    diagNames = Column(String(64), nullable=False, comment='Коды диагнозов')
    chartBeginDate = Column(Date, comment='Р”Р°С‚Р° РЅР°С‡Р°Р»Р° РІРµРґРµРЅРёСЏ РєР°СЂС‚С‹')
    rbInfoSource_id = Column(ForeignKey('rbInfoSource.id', ondelete='SET NULL'), comment='Источник информации {rbInfoSource}')
    notes = Column(TINYTEXT, nullable=False, comment='Примечания')
    IIN = Column(String(15), comment='ИИН')
    isConfirmSendingData = Column(TINYINT(4), comment='Флаг отвечающий за согласие на передачу данных (i3093)')
    isUnconscious = Column(TINYINT(1), server_default=text("0"), comment='Флаг поступившего без сознания')
    filial = Column(INTEGER(10), comment='rbFilials.id Филиал, в котором было установлено значение Client.filial. NULL для всех новых клиентов после обновления, -1 для всех до обновления')
    dataTransferConfirmationDate = Column(Date, comment='Дата согласия на передачу данных')

    # attendingPerson = relationship('Person', primaryjoin='Client.attendingPerson_id == Person.id')
    # bloodType = relationship('RbBloodType')
    # createPerson = relationship('Person', primaryjoin='Client.createPerson_id == Person.id')
    # modifyPerson = relationship('Person', primaryjoin='Client.modifyPerson_id == Person.id')
    # rbInfoSource = relationship('RbInfoSource')

