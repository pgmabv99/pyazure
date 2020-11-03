import datetime
from utz import utz,ddict
from utzexc import UtzExc


class az_data:

    def __init__(self):
        utz.enter2()

    def get_sales_order_val(self, item_id):
        utz.enter2(item_id)
        order1 = {'id': item_id,
                  'account_number': 'Account1',
                  'purchase_order_number': 'PO18009186470',
                  'order_date': datetime.date(2005, 1, 10).strftime('%c'),
                  'total_due': 985.018,
                  'items': [
                      {'order_qty': 1,
                       'product_id': 100,
                       'unit_price': 400,
                       },
                      {'order_qty': 1,
                       'product_id': 200,
                       'unit_price': 800,
                       },
                      {'order_qty': 1,
                       'product_id': 300,
                       'unit_price': 900,
                       },
                  ],
                  'zips': [
                      {'zip': 11111,
                       },
                      {'zip': 22222,
                       },
                  ],
                  'ttl': 60 * 60 * 24 * 30
                  }

        return order1

    def get_sales_order_val_2(self, item_id):
        utz.enter2(item_id)
        # notice new fields have been added to the sales order
        order2 = {'id': item_id,
                  'account_number': 'Account2',
                  'purchase_order_number': 'PO15428132599',
                  'order_date': datetime.date(2005, 7, 11).strftime('%c'),
                  'due_date': datetime.date(2005, 7, 21).strftime('%c'),
                  'shipped_date': datetime.date(2005, 7, 15).strftime('%c'),
                  'total_due': 4893.3929,
                  'items': [
                      {'order_qty': 3,
                       # notice how in item details we no longer reference a ProductId
                       'product_code': 'A-123',
                       # instead we have decided to denormalise our schema and include
                       'product_name': 'Product 1',
                       # the Product details relevant to the Order on to the Order directly
                       'currency_symbol': '$',
                       # this is a typical refactor that happens in the course of an application
                       'currecny_code': 'USD',
                       # that would have previously required schema changes and data migrations etc.
                       'unit_price': 17.1,
                       }
                  ],
                  'ttl': 60 * 60 * 24 * 30
                  }

        return order2

    def get_sales_order_val_list(self):
        order_list = []
        order_list.append(self.get_sales_order_val("SalesOrder1"))
        # order_list.append(self.get_sales_order_val_val_2("SalesOrder2"))
        return order_list

    def get_ppl_table_desc(self):
        table_desc = {
            "table_name": "sales.visits",
            "table_engine": "mss",
            "columns": [
                 {"name": "id", "format": "int", "col_len": 0, "pkey": True, "identity":True}
                ,{"name": "first_name", "format": "varchar", "col_len": 50  }
                ,{"name": "visited_at", "format": "datetime", "col_len": 0 }
                ,{"name": "height", "format": "float", "col_len": 0 }

            ]

        }
        return ddict(table_desc)

    def get_ppl_val_list(self):
        dt1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        val_list = []
        val_list.append("  ( 'Lev', '" + dt1+"', 20.2)")
        val_list.append(", ( 'Whsikey', '" + dt1+"', 10.1)")
        val_list.append(", ( 'alexey', '" + dt1+"', 30.3)")
        return val_list
