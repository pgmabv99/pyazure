# create DB, cointaner

import logging
import os
import json
from datetime import datetime, date
# import pandas as pd


import pyodbc

from az_data import az_data
from utz import utz,ddict
from utzexc import UtzExc


class az_mss:

    def __init__(self):
        utz.enter2()
        logging.basicConfig(level=logging.WARNING,
                            format='%(asctime)s %(message)s')

        # get data obj
        self.az_data1 = az_data()

    def db_con(self):
        utz.enter2()

        server = 'tcp:pgmabv88.database.windows.net'
        database = 'db88'
        username = 'srvadmin'
        password = 'Lena8484'
        con_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + \
            server+';DATABASE='+database+';UID='+username+';PWD=' + password
        print("connecting to ", con_str)

        try:
            self.con = pyodbc.connect(con_str)
        except pyodbc.InterfaceError as e:
            print("Error:================================= connect fail")
            print(e)
            raise UtzExc(0, 0, "DBMS error")

        print(self.con)

        self.crs = self.con.cursor()
        print(self.crs)

    def list_databases(self):
        utz.enter2()

    def re_create_schema(self):
        utz.enter2()
        try:
            q=""
            q=q+"DROP SCHEMA sales"
            print("executing SQL \n", q)
            self.crs.execute(q)
        except pyodbc.ProgrammingError as e:
            print("Error:================================= DROP schema failed ")
            print(e)

        try:
            q=""
            q=q+"CREATE SCHEMA sales  "
            print("executing SQL \n", q)
            self.crs.execute(q)
        except pyodbc.ProgrammingError as e:
            print("Error:================================= CREATE schema failed ")
            print(e)
            raise UtzExc(0, 0, "DBMS error")

    def re_create_table(self, td):
        utz.enter2()
        try:
            q = ""
            q = q+"DROP TABLE " + td.table_name
            print("executing SQL \n", q)
            self.crs.execute(q)
            self.con.commit()
        except pyodbc.ProgrammingError as e:
            print("Error:================================= DROP failed ")
            print(e)
            # raise UtzExc(0, 0, "DBMS error")

        try:
            q = ""
            q = q+"CREATE TABLE " + td.table_name+ "  ( "
            icol=0
            last_col=len(td.columns)-1
            for col in td.columns:
                ddcol=ddict(col)
                q=q+" "+ddcol.name
                q=q+" "+ddcol.format
                col_len=ddcol.col_len
                if col_len !=0 :
                    q=q+"("+str(col_len)+")"
                if ddcol.pkey==True:
                    q=q+" PRIMARY KEY  "
                if ddcol.identity==True:
                    q=q+" IDENTITY (1, 1)"
                if icol==last_col:
                    q=q+")"
                else:
                    q=q+","
                icol +=1


            print("executing SQL \n", q)
            self.crs.execute(q)
            self.con.commit()

        except pyodbc.ProgrammingError as e:
            print("Error:================================= CREATE failed ")
            print(e)
            raise UtzExc(0, 0, "DBMS error")

    def list_containers(self):
        utz.enter2()

    def insert_rows(self, td, val_list):
        utz.enter2()
        try:
            q = ""
            q = q + " INSERT INTO " + td.table_name + "("
            icol=0
            last_col=len(td.columns)-1
            for col in td.columns:
                # for dot notation
                ddcol=ddict(col)
                if ddcol.identity == True :
                    # identity are inserted by system
                    icol +=1
                    continue
                q=q+ddcol.name
                if icol==last_col:
                    q=q+")"
                else:
                    q=q+","
                icol +=1
     
            q = q + " VALUES"
            for ppl in val_list:
                q = q+ppl

            print("executing SQL \n", q)
            self.crs.execute(q)
            self.con.commit()
        except pyodbc.InterfaceError as e:
            print("Error:================================= insert failed ")
            print(e)
            raise UtzExc(0, 0, "DBMS error")
    
    # read table based on td dict. result in into panda(pd) and json(js) -(duplicate)
    def query_rows(self, td):
        utz.enter2("QUery============")
        try:

            # =============
            q = ""
            q = q+"SELECT * FROM " + td.table_name
            print("executing SQL \n", q)
            self.crs.execute(q)
         

            #build column desciptor
            js_col_list=[]
            for row in self.crs.description:

                col=ddict({})
                col.name=row[0]
                sql_format=row[1]
                if sql_format == int:
                    df_col_form = "int64"
                elif sql_format == float:
                    # todo why float doea not work, converted to in in df
                    # df_col_form="float"
                    df_col_form = "object"
                elif sql_format == str:
                    df_col_form = "string"
                elif sql_format == datetime:
                    df_col_form = "datetime64"
                else:
                    df_col_form = "object"
                # todo use dataframe as generic format for now
                col.df_col_format=df_col_form
                js_col_list.append(col)

            # # build df
            # df = pd.DataFrame()
            # todo pando too slow to build in docker
            df={}
            # for col  in js_col_list:
            #     # add header(olummn name) to pandas
            #     df[col.name] = ""
            #     # build dict with single entry
            #     df_col_formats = {}
            #     df_col_formats[col.name] =  col.df_col_format
            #     # apply to df
            #     df = df.astype(df_col_formats)

            # # this prevents conversion of int to float OMG
            # df = df.convert_dtypes()
            # print("df before load\n", df.dtypes)

            js_row_list = []
            row = self.crs.fetchone()
            irow = 0
            while row:
                icol = 0
                js_row = {}
                for col_val in row:

                    # # add cell to df
                    # df.at[irow, js_col_list[icol].name] = col_val

                    # add attribute to json
                    col_val1 = col_val
                    # todo convert datatime autoatically
                    if isinstance(col_val, (datetime, date)):
                        col_val1 = col_val.isoformat()
                    # todo hack to convert id to str for cosmos
                    if icol == 0:
                        col_val1 = str(col_val)
                    js_row[js_col_list[icol].name] = col_val1

                    icol += 1
                # append to json array
                js_row_list.append(js_row)
                # next row
                row = self.crs.fetchone()
                irow += 1
            # end loop
        except pyodbc.InterfaceError as e:
            print("Error:================================= select/fetch failed ")
            print(e)
            raise UtzExc(0, 0, "DBMS error")

        return df, js_row_list, js_col_list
