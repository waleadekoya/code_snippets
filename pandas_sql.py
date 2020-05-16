import pandas as pd
from pandas import DataFrame
from pandasql import sqldf


class Example:

    def __init__(self):
        self.df = pd.read_csv('/path/to/file.csv')
        self.new_df = self.df.loc[self.df.color == 'yellow', :]
        self.thin_table = None

        print(self.ingest_new_table())
        # print(self.__dict__)

    def __call__(self, *args, **kwargs):
        return Example()

    def register_df(self, df: DataFrame, table_name: str):
        self.__dict__[table_name] = df

    def ingest_new_table(self):
        self.thin_table = self.pysqldf("select * from df where color = 'black'")
        return self.pysqldf("select * from df union select * from thin_table union all select * from new_df")

    def pysqldf(self, query):
        """ access the class namespace to get table names"""
        return sqldf(query, env=self.__dict__)


if __name__ == '__main__':
    Example()
