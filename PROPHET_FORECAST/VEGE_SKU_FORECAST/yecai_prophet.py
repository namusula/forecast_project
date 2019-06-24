from fbprophet import Prophet
from pyhive import hive
import platform
from sqlalchemy import create_engine
from datetime import *
import pandas as pd
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
        read_sql = '''  select warehouse_id,sku_id
                        from(select t1.warehouse_id,t1.sku_id,t1.befo,t2.afte,t3.total,t3.o_w
                        from(select warehouse_id,sku_id,count(distinct datekey) as befo
                        from adb.adb_forecast_pro_warehouse_sku_weight
                         where dt >= '2019-03-01' and dt < '2019-06-01' and city_id = 1 and class1_id = 372 and class2_id = 1
                        group by warehouse_id,sku_id)t1
                        left join
                        (select warehouse_id,sku_id,count(distinct datekey) as afte
                        from adb.adb_forecast_pro_warehouse_sku_weight
                         where dt >= '2019-06-01' and city_id = 1 and class1_id = 372 and class2_id = 1
                        group by warehouse_id,sku_id)t2
                        on t1.warehouse_id = t2.warehouse_id and t1.sku_id = t2.sku_id
                        left join
                        (select warehouse_id,sku_id,count(distinct datekey) as total,avg(order_weight) o_w
                        from adb.adb_forecast_pro_warehouse_sku_weight
                         where dt >= '2019-03-01' and city_id = 1 and class1_id = 372 and class2_id = 1
                        group by warehouse_id,sku_id)t3
                        on t1.warehouse_id = t3.warehouse_id and t1.sku_id = t3.sku_id) t
                        where afte is not null and total >= 30  '''
        w_data = pd.read_sql(read_sql, con=self.conn)
        w_data.colunms = ['warehouse_id','sku_id']
        w_data.dropna(inplace=True)
        w = np.array(w_data)
        w_new = w.tolist()



        return w_new

    def GetData(self):

        read_sql = '''select datekey,warehouse_id,sku_id,order_weight
                      from adb.adb_forecast_pro_warehouse_sku_weight
                      where city_id = 1 and class1_id = 372 and class2_id = 1 and  dt > '2019-03-01' '''

        data = pd.read_sql(read_sql, con=self.conn)
        data = pd.DataFrame(data, columns=['datekey', 'warehouse_id', 'sku_id', 'order_weight'])
        data['order_weight'] = data['order_weight'].apply(pd.to_numeric)
        data['datekey'] = pd.to_datetime(data['datekey'])
        data = data.sort_values(by='datekey')

        return data


    def GetSkuData(self,data,warehouse,sku,date):
        print(warehouse,sku)
        sku_data = data[(data['sku_id'] == sku) & (data['warehouse_id'] == warehouse)]
        # print(sku_data)
        before_data = sku_data[(sku_data['datekey'] < date)]
        new_data = before_data.loc[:, ['datekey', 'order_weight']]
        new_data = new_data.rename(columns={'datekey': 'ds', 'order_weight': 'y'})
        return new_data

    def prophet(self,data):

        data['y'] = np.log(data['y'])
        April_sales = pd.DataFrame({
            'holiday': 'April sales',
            'ds': pd.to_datetime(['2019-04-08', '2019-04-10', '2019-04-11', '2019-04-12', '2019-04-13', '2019-04-14']),
            'lower_window': 0,
            'upper_window': 0, })

        May_Day = pd.DataFrame({
            'holiday': 'May',
            'ds': pd.to_datetime(['2019-05-01', '2019-05-02', '2019-05-03']),
            'lower_window': 0,
            'upper_window': 0, })

        June_Sales = pd.DataFrame({
            'holiday': 'May',
            'ds': pd.to_datetime(['2019-06-07']),
            'lower_window': 0,
            'upper_window': 0, })

        # changepoints = ['2019-05-24', '2019-05-20', '2019-05-10', '2019-05-12',
        #                 '2019-05-19', '2019-05-26', '2019-05-04']

        holidays = pd.concat((April_sales, May_Day, June_Sales))

        m = Prophet(holidays=holidays, holidays_prior_scale=1)
        # m = Prophet()
        m.fit(data)
        future = m.make_future_dataframe(periods=1, freq='D')
        future.tail()
        forecast = m.predict(future)
        pred_data = forecast[['ds', 'yhat']]
        pred_data['yhat'] = np.exp(pred_data['yhat'])
        pred_data = pred_data.tail(1)
        pred_data = pred_data.rename(columns={'ds': 'datekey', 'yhat': 'forecast_weight'})

        return pred_data

    def main(self):
        w_s = self.warehouse()
        print(w_s)
        # w_s = [(45, 1260), (1, 2511), (1, 3165)]

        # orderdate = ['2019-06-01', '2019-06-02', '2019-06-03', '2019-06-04', '2019-06-05', '2019-06-06', '2019-06-07',
        #              '2019-06-08', '2019-06-09', '2019-06-10', '2019-06-11'
        #     , '2019-06-12', '2019-06-13', '2019-06-14', '2019-06-15', '2019-06-16', '2019-06-17', '2019-06-18',
        #              '2019-06-19']
        orderdate = ['2019-06-15','2019-06-16', '2019-06-17', '2019-06-18','2019-06-19']
        # orderdate = [datetime.now().date()]
        pred = []
        all_data = self.GetData()
        print(all_data)
        for d in orderdate:
            print(d)
            for i in w_s:
                print(i)
                new = self.GetSkuData(all_data,i[0],i[1],d)
                print(new)

                pred_data = self.prophet(new)
                print(pred_data)

                pred_data['warehouse_id'] = i[0]
                pred_data['sku_id'] = i[1]
                pred_data['datekey'] = pred_data['datekey'].astype('str')
                pred.append(pred_data)
        forecast = pd.concat(pred)

        return forecast


    def write_hive(self,data):
        data.to_sql(name="adb_vege_left_sku_prophet_forecast_test", schema='adb', con=self.write_hive_engine,if_exists='replace', index=False, method='multi')
        finish = print("already write")

        return finish




