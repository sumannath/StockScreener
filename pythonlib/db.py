import os
import os

from peewee import PostgresqlDatabase


class DatabaseSingleton:
    _instance = None

    @classmethod
    def get_db_instance(cls):
        if cls._instance is None:
            cls._instance = PostgresqlDatabase(
                database=os.environ['POSTGRES_DB'],
                user=os.environ['POSTGRES_USER'],
                password=os.environ['POSTGRES_PASSWORD'],
                host=os.environ['POSTGRES_HOST'],
                port=os.environ['POSTGRES_PORT'],
            )

        return cls._instance