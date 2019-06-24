import platform
from pyhive import hive
from sqlalchemy import create_engine
import datetime
import pandas as pd
from fbprophet import Prophet
import util_db as util
import numpy as np

class Trainer:
    def __init__(self):
        if platform.system() == "Windows":
            self.test_env = True
        else:
            self.test_env = False
        # host = "192.168.254.48"  # 测试环境
        self.host = "10.5.2.40"  # 线上环境
        self.port = 10000
        self.db_usr = "ai"
        self.password = "ai"
        try:
            self.conn = hive.Connection(host=self.host, port=self.port, username=self.db_usr)
            self.cur = self.conn.cursor()
            writehost = f"ai@{self.host}"
            db_info = {'user': 'ai', 'password': 'ai', 'host': self.host, 'port': 10000, 'database': 'fsp'}
            self.write_hive_engine = create_engine('hive://%(user)s@%(host)s:%(port)d/%(database)s?' % db_info,
                                                   encoding='utf-8')
        except:
            print("connection to hive failed")
            pass
        # self.configdf=pd.DataFrame()
        self.cos = []
        try:
            mysqlconn = create_engine("mysql+mysqldb://root:@vRQyaN@10.5.2.17:3306/membi?charset=utf8")
            db_info = {'user': 'root', 'password': '@vRQyaN]', 'host': '10.5.2.17', 'port': 3306, 'database': 'membi'}
            self.mysqlengine = create_engine(
                'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info,
                encoding='utf-8')
        except:
            print("connection to mysql failed")

    def city_class(self):
        read_sql = '''  select city_id,cx_class_1_id
                        from mjyx_dwd.dwd_order_item_info_1d 
                        where dt >= 20190220 and status not in (0) and pay_status not in (10)
                        group by city_id,cx_class_1_id '''
        w_data = pd.read_sql(read_sql, con=self.conn)
        w_data.colunms = ['city_id','class_id']
        w_data.dropna(inplace=True)
        w = w_data['city_id'].tolist()

        return w


    def read_city_data(self,city_id,class_id):
        read_sql = '''select business_date,city_id,cx_class_1_id,sum(total_amount) order_money,count(distinct order_id) order_list
                      from mjyx_dwd.dwd_order_item_info_1d 
                      where dt >= 20190220 and status not in (0) and pay_status not in (10)
                      group by business_date,city_id,cx_class_1_id '''.format(city_id,class_id)
        print(read_sql)
        data = pd.read_sql(read_sql, con=self.conn)
        # print(data)
        data.columns = ['ds', 'warehouse_id', 'line_id','y']
        data['warehouse_id'] = data['warehouse_id'].apply(pd.to_numeric)
        data['line_id'] = data['line_id'].apply(pd.to_numeric)
        data['order_money'] = data['order_money'].apply(pd.to_numeric)
        data['order_datekey'] = data['order_datekey'].map(str)
        data['order_datekey'] = pd.to_datetime(data['order_datekey'])
        type = data.info()
        # print(type)

        return data
