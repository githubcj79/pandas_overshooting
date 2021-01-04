#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
from decouple import config
from sqlalchemy import create_engine
import pandas as pd
import pymysql

ENV = config('ENV')
MYSQL_CONF = config('MYSQL_CONF')

def mysql_df(query_=None):

    global DB_STR_CONNECTION

    section = 'mysql_' + ENV
    config_dict = configparser.ConfigParser()
    config_dict.read(MYSQL_CONF)

    host = config_dict[section]['host']
    user = config_dict[section]['user']
    password = config_dict[section]['password']
    database = config_dict[section]['database']
    port = config_dict[section]['port']

    # -------------------------------------
    db_str_connection = ("mysql+pymysql:"
                        f"//{user}:{password}@{host}:{port}/{database}")
    sql_engine = create_engine(db_str_connection, pool_recycle=3600)
    db_connection = sql_engine.connect()
    # table: prs_lte_hour

    # select * from prs_lte_hour where (dateid_date between '2020-12-02' and '2020-12-02') and dateid_hour = '20';
    # query_ = ("select * from prs_lte_hour where "
    #     "(dateid_date between '2020-12-02' "
    #     "and '2020-12-02') and dateid_hour = '20' ")
    df_ = pd.read_sql(query_, db_connection);
    # pd.set_option('display.expand_frame_repr', False)
    # print(df_)
    db_connection.close()

    return df_

    # df_.to_json(r'C:\cygwin64\home\carlos\lab\pandas-mysql\data\prs_lte_hour.json',orient='records')
    # -------------------------------------

def main():
    global DB_STR_CONNECTION

    section = 'mysql_' + ENV
    config_dict = configparser.ConfigParser()
    config_dict.read(MYSQL_CONF)

    host = config_dict[section]['host']
    user = config_dict[section]['user']
    password = config_dict[section]['password']
    database = config_dict[section]['database']
    port = config_dict[section]['port']

    # -------------------------------------
    db_str_connection = ("mysql+pymysql:"
                        f"//{user}:{password}@{host}:{port}/{database}")
    sql_engine = create_engine(db_str_connection, pool_recycle=3600)
    db_connection = sql_engine.connect()
    # table: prs_lte_hour

    # select * from prs_lte_hour where (dateid_date between '2020-12-02' and '2020-12-02') and dateid_hour = '20';
    query_ = ("select * from prs_lte_hour where "
        "(dateid_date between '2020-12-02' "
        "and '2020-12-02') and dateid_hour = '20' ")
    frame = pd.read_sql(query_, db_connection);
    pd.set_option('display.expand_frame_repr', False)
    print(frame)
    db_connection.close()

    frame.to_json(r'C:\cygwin64\home\carlos\lab\pandas-mysql\data\prs_lte_hour.json',orient='records')
    # -------------------------------------


if __name__ == '__main__':
    main()
