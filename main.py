# -*- coding:utf-8 -*-
import os
import pandas as pd
#pip instal impyla
from impala.dbapi import connect
from impala.util import as_pandas


def get_con():
    return connect(host='<神策服务器IP地址>',
                   port=21050, auth_mechanism='nosasl', user='神策账号',database='rawdata')

def hive_sql(sql):
    with get_con() as con:
        cursor = con.cursor()
        cursor.execute(sql)
        # pd = as_pandas(cursor)
        # print(pd)
        cursor.close()
def hive_sql_df(sql):
    with get_con() as con:
        cursor = con.cursor()
        cursor.execute(sql)
        df = as_pandas(cursor)
        print(df)
        cursor.close()
        return df