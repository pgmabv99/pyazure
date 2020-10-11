# create DB, cointaner
# load items
# list
# query with joins/hiearchy/mulitpath 

import logging
import os
import json


from azure.cosmos import CosmosClient, exceptions
from azure.cosmos.partition_key import PartitionKey

from az_data import az_data
from utz import utz
from utzexc import UtzExc


class az_cos:

    def __init__(self):
        utz.enter2()
        logging.basicConfig(level=logging.WARNING,
                            format='%(asctime)s %(message)s')

        self.dbid = "dbid1"
        logging.info("starting %s  %s ", self.dbid)

    def db_con(self):
        utz.enter2()
        self.client = CosmosClient("https://pgmabv99.documents.azure.com:443/",
                                   {'masterKey': "qG1DL0mLFtFCfXl0N8jLK9TDYAHRJn4bok2ZqmJvP6RemJDn3oQ2lWostz2EgFJ0bEOVEY4wvXqOfF8k8hp09Q=="})
        logging.info("connection established")  
        print(self.client)

    def list_databases(self):
        utz.enter2()

        databases_list = list(self.client.list_databases())
        for database in databases_list:
            print(database['id'])

    def re_create_database(self):
        utz.enter2()
        try:
            self.client.delete_database(database=self.dbid)
            print('Database  deleted: ', self.dbid)

        except exceptions.CosmosResourceNotFoundError:
            print('A database with id  not found:', self.dbid)

        try:
            self.db_obj = self.client.create_database(id=self.dbid)
            print('Database  created :', self.dbid)

        except exceptions.CosmosResourceExistsError:
            print('A database with id  already exists:', self.dbid)

    def re_create_container(self,td):
        utz.enter2()
        # //todo softcode id
        self.partition_key = PartitionKey(path='/id', kind='Hash')
        #todo container name may not have . ??
        cnid=td.table_name.replace(".","X")
        try:
            self.db_obj.delete_container(container=cnid)
            print('container  deleted: ', cnid)
        except exceptions.CosmosResourceNotFoundError:
            print('A container  with id  not found:', cnid)

        try:
            cnt_obj = self.db_obj.create_container(
                id=cnid, partition_key=self.partition_key)
            print('Container with id  created', cnid, cnt_obj)
        except exceptions.CosmosResourceExistsError:
            print('A container with id already exists:', cnid)
        # todo move inside try ??
        return cnt_obj

    def list_containers(self):
        utz.enter2()
        containers = list(self.db_obj.list_containers())
        for container in containers:
            print(container['id'])

    def create_items(self,cnt_obj,js):
        utz.enter2()
        for jsa in js:
            resp=cnt_obj.create_item(body=jsa)
            print("item created _rid =",resp["_rid"])


    def list_items(self,cnt_obj):
        utz.enter2("List ===========")
        item_list = list(cnt_obj.read_all_items(max_item_count=10))
        for item in item_list:
            print(json.dumps(item, indent=True))

    def query_items(self,td, cnt_obj):
        utz.enter2("QUery============")
        cnid=td.table_name.replace(".","X")
        try:
            # =============read from root 
            q = ""
            q = q+"SELECT "
            q = q+" *"
            q=q+" FROM "  + cnid + " AS a"
           
            # =============read from root with filter contains on child instance. full set of child value is supplied
            # q = ""
            # q = q+"SELECT "
            # q = q+" a.id "
            # q = q +"  ,ARRAY_CONTAINS(a.items, {\"order_qty\": 1, \"product_id\": 200,\"unit_price\": 800 })"
            # q=q+" FROM "  + cnid + " AS a"

            # # =============read from root with multipath
            # q = ""
            # q = q+"SELECT "
            # q = q+"a.id, a.zips, a.items"
            # # q = q +"  ,ARRAY_CONTAINS(a.items, {\"order_qty\": 1, \"product_id\": 200,\"unit_price\": 800 })"
            # q=q+" FROM "  + cnid + " AS a"
            # q=q+" WHERE ARRAY_CONTAINS(a.items, {\"order_qty\": 1, \"product_id\": 200,\"unit_price\": 800})"

            #==============read from descendent via IN filter = . any field can be selected
            # q = ""
            # q = q+" SELECT "
            # q = q+" * "
            # q = q+" FROM a "
            # q = q+" IN "+ cnid+  ".items"
            # q = q+" WHERE a.product_id >=200 "

            #==============read from descendent via JOIN . flatten 
            # q = ""
            # q = q+"SELECT "
            # q = q+" a.id, b.product_id"
            # q = q+" FROM "  + cnid + " AS a"
            # q = q+" JOIN  b IN  a.items"
            # q = q+" WHERE b.product_id >=200 "


            print("executing SQL \n", q)


            item_list=cnt_obj.query_items(
                 query=q, enable_cross_partition_query=True
            )
            for item in item_list:
                print(json.dumps(item, indent=True))
        except exceptions.CosmosHttpResponseError as e:
            print("Error:================================= select failed 3")
            print(e.exc_msg)
            raise UtzExc(0, 0, "DBMS error")
