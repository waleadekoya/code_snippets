from sqlalchemy import create_engine, Column, Table, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String
from sqlalchemy_utils import database_exists, create_database

from config import CONNECTION_STRING, db_name

DeclarativeBase = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    create_database_if_not_exist(db_name)
    return create_engine(CONNECTION_STRING)


def create_table(engine):
    DeclarativeBase.metadata.create_all(engine)


def create_database_if_not_exist(dbname):
    # https://stackoverflow.com/a/30971098
    engine = create_engine('mysql://wale:omoyemiHouse14+@localhost/' + dbname)
    if not database_exists(engine.url):
        create_database(engine.url)
    print(database_exists(engine.url))


class JobsDB(DeclarativeBase):
    __tablename__ = "jobs_db"

    id = Column(Integer, primary_key=True)
    job_type = Column('job_type', String(100), nullable=True)
    job_link = Column('job_link', String(250))
    location = Column('location', String(150))
    title = Column('title', String(250), nullable=True)
    salary = Column('salary', String(250), nullable=True)
    advertiser = Column('advertiser', String(250), nullable=True)
