# Azure test

from az_cos import az_cos
from az_mss import az_mss
from az_data import az_data
from utz import utz,ddict
from utzexc import UtzExc
import json


class az_test:
    def __init__(self):
        # get data obj
        self.az_data1 = az_data()

    def test_cos(self):
        try:
            cos_obj = az_cos()
            cos_obj.db_con()
            cos_obj.re_create_database()
            cos_obj.list_databases()
            td=self.az_data1.get_ppl_table_desc()

            # create container get handle
            cnt_obj = cos_obj.re_create_container(td)
            cos_obj.list_containers()

            js = self.az_data1.get_sales_order_val_list()
            cos_obj.create_items(cnt_obj, js)

            cos_obj.query_items(td, cnt_obj)
            cos_obj.list_items(cnt_obj)
        except UtzExc:
            print("............aborting")

    def test_mss(self):
        try:
            mss_obj = az_mss()
            mss_obj.db_con()

            td=self.az_data1.get_ppl_table_desc()
            #todo to be run once 
            mss_obj.re_create_schema()
            mss_obj.re_create_table(td)

            val_list = self.az_data1.get_ppl_val_list()
            mss_obj.insert_rows(td, val_list)
            resp = mss_obj.query_rows(td)
            df = resp[0]
            js = resp[1]
            # print("df after\n", df.dtypes)
            print("df after\n", df)
            utz.jprint(js)

        except UtzExc:
            print("............aborting")

    def test_mss_to_cos(self):
        try:
            mss_obj = az_mss()
            mss_obj.db_con()

            td=self.az_data1.get_ppl_table_desc()
            #todo to be run once 
            mss_obj.re_create_schema()
            mss_obj.re_create_table(td)

            val_list = self.az_data1.get_ppl_val_list()
            mss_obj.insert_rows(td, val_list)

            # get rows in js_row_list
            resp = mss_obj.query_rows(td)
            js_row_list = resp[1]

            cos_obj = az_cos()
            cos_obj.db_con()
            cos_obj.re_create_database()
            cnt_obj = cos_obj.re_create_container(td)
            # create items
            cos_obj.create_items(cnt_obj, js_row_list)
            cos_obj.query_items(td, cnt_obj)

        except UtzExc:
            print("............aborting")

    def mss_to_html(self):
        az_data1 = az_data()
        mss_obj = az_mss()
        mss_obj.db_con()

        td=self.az_data1.get_ppl_table_desc()
        resp = mss_obj.query_rows(td)
        js_row_list = resp[1]
        js_col_list = resp[2]

        # add callback
        cb_list=[]
        for js_row in js_row_list:
            cb_list.append('https://www.google.com/search?q='+ddict(js_row).first_name )

        return js_row_list,js_col_list,cb_list



