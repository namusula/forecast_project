import platform
from pyhive import hive
from sqlalchemy import create_engine
import datetime
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA
import statsmodels.api as sm
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

    def warehouse(self):
        read_sql = '''  select warehouse_id
                               from app.app_warehouse_line_gmv
                               where dt > 20190325 and warehouse_id not in (10,1388)
                               group by warehouse_id '''
        w_data = pd.read_sql(read_sql, con=self.conn)
        w_data.colunms = ['warehouse_id', 'line_id']
        w_data.dropna(inplace=True)
        w = w_data['warehouse_id'].tolist()

        return w

    def read_warehouse_data(self,warehouse_id,line_id):
        read_sql = '''select
                         concat((substring(cast(order_datekey as string),1,4)),'-',
                        (substring(cast(order_datekey as string),5,2)),'-',
                        (substring(cast(order_datekey as string),7,2))),
                        warehouse_id,line_id,order_money
                      from app.app_warehouse_line_gmv
                      where warehouse_id = {} and line_id = {} and dt >= 20190325 '''.format(warehouse_id,line_id)
        print(read_sql)
        data = pd.read_sql(read_sql, con=self.conn)
        # print(data)
        data.columns = ['order_datekey', 'warehouse_id', 'line_id','order_money']
        data['warehouse_id'] = data['warehouse_id'].apply(pd.to_numeric)
        data['line_id'] = data['line_id'].apply(pd.to_numeric)
        data['order_money'] = data['order_money'].apply(pd.to_numeric)
        data['order_datekey'] = data['order_datekey'].map(str)
        data['order_datekey'] = pd.to_datetime(data['order_datekey'])
        type = data.info()
        # print(type)

        return data

    def get_para(self,w,l):
        read_sql = '''select warehouse_id,line_id,p,q from adb.adb_gmv_forecast_arima_para'''
        para_data = pd.read_sql(read_sql, con=self.conn)
        result_para = para_data[(para_data['warehouse_id'] == w ) & (para_data['line_id']== l )]
        print(result_para)

        return result_para


    def model(self, data, p, d, q, w, l):

        print(p, d, q)
        model = ARIMA(data['order_money'].dropna(), order=(p, d, q))
        results_AR = model.fit(disp=-1)
        oneday = results_AR.forecast(steps=1)
        nextday = oneday[0][0]
        predict = results_AR.predict(typ='levels')
        data['forecast_gmv'] = predict

        max_date = data['order_datekey'].max()
        delta = datetime.timedelta(days=1)
        newdate = max_date + delta
        new_row = pd.DataFrame(
            {'order_datekey': newdate, 'warehouse_id': w, 'line_id': l, 'order_money': '0', 'forecast_gmv': nextday},
            index=[1])
        data = data.append(new_row, ignore_index=True)

        return data
    
    def main(self):

        d = 1
        warehouse_id = self.warehouse()
        # warehouse_id = [1,47]
        line_id = [1, 2, 3]
        GMV = []

        for w in warehouse_id:
            print(w)
            for l in line_id:
                print(l)
                data = self.read_warehouse_data(w,l)
                para = self.get_para(w,l)
                para_to_ndarray = np.array(para)
                para_list = para_to_ndarray.tolist()
                print(para_list[0])
                try:
                    pred_result = self.model(data,para_list[0][2],d,para_list[0][3],w,l)
                    pred_result['order_datekey'] = pred_result['order_datekey'].astype('str')
                    GMV.append(pred_result)
                except:
                    pass
        GMV_total = pd.concat(GMV)

        return GMV_total


    def write_hive(self,data):
        data.to_sql(name="fsp_gmv_forecast_daily", schema='fsp', con=self.write_hive_engine,if_exists='replace', index=False, method='multi')
        finish = print("already write")

        return finish

