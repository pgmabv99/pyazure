# Azure test

from az_cos import az_cos
from az_mss import az_mss
from az_data import az_data
from utz import utz
from utzexc import UtzExc
import json


class test:
    def __init__(self):
        # get data obj
        self.az_data1 = az_data()

    def test_cos(self):
        try:
            cos_obj = az_cos()
            cos_obj.db_init()
            cos_obj.re_create_database()
            cos_obj.list_databases()
            

            # create container get handle
            cnt_obj = cos_obj.re_create_container("cnid1")
            cos_obj.list_containers()

            js = self.az_data1.get_sales_order_list()
            cos_obj.create_items(cnt_obj, js)

            cos_obj.query_items("cnid1", cnt_obj)
            cos_obj.list_items(cnt_obj)
        except UtzExc:
            print("............aborting")

    def test_mss(self):
        try:
            mss_obj = az_mss()
            mss_obj.db_init()
            mss_obj.re_create_table()

            ppl_list = self.az_data1.get_ppl_list()
            mss_obj.insert_rows("sales.visits", ppl_list)
            resp = mss_obj.query_rows("sales.visits")
            df = resp[0]
            js = resp[1]
            print("df after\n", df.dtypes)
            print("df after\n", df)
            utz.jprint(js)

        except UtzExc:
            print("............aborting")

    def test_mss_to_cos(self):
        try:
            mss_obj = az_mss()
            mss_obj.db_init()
            mss_obj.re_create_table()

            cos_obj = az_cos()
            cos_obj.db_init()
            cos_obj.re_create_database()
            cnt_obj = cos_obj.re_create_container("cnid1")

            ppl_list = self.az_data1.get_ppl_list()
            mss_obj.insert_rows("sales.visits", ppl_list)
            # get rows in js
            resp = mss_obj.query_rows("sales.visits")
            js = resp[1]
            # create items
            cos_obj.create_items(cnt_obj, js)
            cos_obj.query_items("cnid1", cnt_obj)

        except UtzExc:
            print("............aborting")


test1 = test()
# test1.test_mss()
# test1.test_mss()
test1.test_mss_to_cos()
