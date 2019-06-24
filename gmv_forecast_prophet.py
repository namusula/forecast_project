import platform
from pyhive import hive
from sqlalchemy import create_engine
import datetime
import pandas as pd
from fbprophet import Prophet
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

    def GetData(self):

        read_sql = '''select
                         concat((substring(cast(order_datekey as string),1,4)),'-',
                        (substring(cast(order_datekey as string),5,2)),'-',
                        (substring(cast(order_datekey as string),7,2))) datekey,
                        warehouse_id,line_id,order_money
                      from app.app_warehouse_line_gmv
                      where  dt >= 20190325 '''

        data = pd.read_sql(read_sql, con=self.conn)
        data = pd.DataFrame(data, columns=['datekey', 'warehouse_id', 'line_id', 'order_money'])
        data['order_money'] = data['order_money'].apply(pd.to_numeric)
        data['datekey'] = pd.to_datetime(data['datekey'])
        data = data.sort_values(by='datekey')

        return data

    def GetLineData(self,data,warehouse,line,date):
        print(warehouse,line)
        line_data = data[(data['line_id'] == line) & (data['warehouse_id'] == warehouse)]
        # print(sku_data)
        before_data = line_data[(line_data['datekey'] < date)]
        new_data = before_data.loc[:, ['datekey', 'order_money']]
        new_data = new_data.rename(columns={'datekey': 'ds', 'order_money': 'y'})
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
        pred_data = pred_data.rename(columns={'ds': 'datekey', 'yhat': 'forecast_money'})

        return pred_data

    def main(self):
        w = self.warehouse()
        print(w)
        line = [1,2,3]
        orderdate = ['2019-06-23']
        # orderdate = [datetime.now().date()]
        pred = []
        all_data = self.GetData()
        # print(all_data)
        for d in orderdate:
            print(d)
            for i in w:
                for l in line:
                    print(i)
                    new = self.GetLineData(all_data,i,l,d)
                    print(new)

                    pred_data = self.prophet(new)
                    print(pred_data)

                    pred_data['warehouse_id'] = i
                    pred_data['line_id'] = l
                    pred_data['datekey'] = pred_data['datekey'].astype('str')
                    pred.append(pred_data)
        forecast = pd.concat(pred)

        return forecast


    def write_hive(self,data):
        data.to_sql(name="adb_gmv_line_forecast_prophet", schema='adb', con=self.write_hive_engine,if_exists='replace', index=False, method='multi')
        finish = print("already write")

        return finish
