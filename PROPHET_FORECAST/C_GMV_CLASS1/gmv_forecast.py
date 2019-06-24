import pandas as pd
from fbprophet import Prophet
import util_db as util
import numpy as np
import matplotlib.pyplot as plt


def wareline():
    db = util.Db("local")
    sql = '''select distinct city_id,cx_class_1_id from mjyx where city_id in (77,143,3,30,145,9,7,6,12,4,271) and cx_class_1_id > 0'''
    ori_date = db.getBySql(sql)
    w_l = [i for i in ori_date]
    return w_l


def GetData(warehouse,line):
    db = util.Db("local")
    sql = '''select business_date,order_money/100 from mjyx where city_id = {0} and cx_class_1_id = {1} and business_date < 20190525   order by business_date '''.format(warehouse,line)
    ori_date = db.getBySql(sql)
    a = [i for i in ori_date]
    data = pd.DataFrame(a,columns=['ds','y'])
    # data['warehouse_id'] = data['warehouse_id'].apply(pd.to_numeric)
    data['y'] = data['y'].apply(pd.to_numeric)
    data['ds'] = pd.to_datetime(data['ds'])
    data = data.sort_values(by = 'ds')
    # print(data)

    return data

def prophet(data,w,l):

    data['y'] = np.log(data['y'])
    # print(data['y'])

    Jan_sales= pd.DataFrame({
                'holiday':'Jan_sales',
                'ds':pd.to_datetime(['2019-01-06','2019-01-10','2019-01-13']),
                'lower_window' : 0,
                'upper_window' : 0,})

    Spring_Festival= pd.DataFrame({
                'holiday':'spring festivel',
                'ds':pd.to_datetime(['2019-03-02','2019-03-03','2019-03-05','2019-03-06','2019-03-07','2019-03-08','2019-03-12','2019-03-18','2019-03-18','2019-03-19','2019-03-20','2019-03-21','2019-03-26','2019-03-29']),
                'lower_window' : 0,
                'upper_window' : 0,})

    April_sales= pd.DataFrame({
                'holiday':'April sales',
                'ds':pd.to_datetime(['2019-04-22','2019-04-25','2019-04-11','2019-04-28','2019-04-29']),
                'lower_window' : 0,
                'upper_window' : 0,})

    May_Day= pd.DataFrame({
                'holiday':'May',
                'ds':pd.to_datetime(['2019-05-01','2019-05-02','2019-05-03','2019-05-07','2019-05-10','2019-05-14','2019-05-17','2019-05-21','2019-05-24','2019-05-28','2019-05-30','2019-05-31']),
                'lower_window' : 0,
                'upper_window' : 0,})

    June_Sales = pd.DataFrame({
                'holiday': 'June',
                'ds': pd.to_datetime(['2019-06-09', '2019-06-10', '2019-06-11','2019-06-12','2019-06-13','2019-06-14','2019-06-15','2019-06-16','2019-06-08']),
                'lower_window': 0,
                'upper_window': 0, })


    holidays = pd.concat((Spring_Festival,April_sales,May_Day,June_Sales))
    m = Prophet(holidays=holidays,holidays_prior_scale=20)
    # m = Prophet(changepoint_prior_scale=20)
    model = m.fit(data)
    future = m.make_future_dataframe(periods = 15,freq = 'D')
    # future.tail()
    forecast = m.predict(future)
    # print(forecast[['ds','yhat','yhat_lower','yhat_upper']])
    # m.plot(forecast)

    # m.add_country_holidays()

    # x1 = forecast['ds']
    # y1 = forecast['yhat']

    new_data = forecast[['ds','yhat']]
    new_data['yhat'] = np.exp(new_data['yhat'])
    # new_data['warehouse_id'] = w
    # new_data['line'] = l

    # m.plot_components(forecast).show()
    # plt.plot(x1,y1)
    # plt.show()
    return new_data


def merge(pred_data,warehouse,line):
    db = util.Db("local")
    sql = ''' select business_date,city_id,cx_class_1_id,order_money/100 from mjyx where city_id = {0} and cx_class_1_id = {1} and business_date <= 20190610'''.format(warehouse,line)
    ori_date = db.getBySql(sql)
    a = [i for i in ori_date]
    data = pd.DataFrame(a, columns=['ds','city_id','cx_class_1_id','value'])
    # data['warehouse_id'] = data['warehouse_id'].apply(pd.to_numeric)
    data['value'] = data['value'].apply(pd.to_numeric)
    data['ds'] = pd.to_datetime(data['ds'])
    data = data.sort_values(by='ds')
    # print(data)

    forecast_data = pred_data

    new_data = pd.merge(data,forecast_data)
    # new_data['mape'] = abs(new_data['yhat'] - new_data['value'])/new_data['value']
    # tail_data = new_data.tail(20)
    # mean_mape = tail_data['mape'].mean()
    # print(tail_data)
    # print(mean_mape)

    # x2 = new_data['ds']
    # y2 = new_data[['value','yhat']]
    #
    #
    # plt.plot(x2,y2)
    # plt.show()
    return new_data
# plot()

if __name__ == '__main__':
    # w_l = wareline()
    GMV = []
    pred = []
    para = []
    # n = 1
    # h = 1
    # for i in range(1,2,1):
    #
    #     for k in range(1,20,2):
    #
    #         para_one = i,k
    #         para.append(para_one)
    #         # k = k+2
    #     # i = i + 2
    ware = [77,3,143,9,7,30]
    line = [379,3012,372,376,3015]
    mape_result = []
    for w in ware:
        for l in line:
            # for p in para:
                data = GetData(w,l)
                pred_data = prophet(data,w,l)
                # print(pred_data)
                final_data = merge(pred_data,w,l)
                # mape = merge(pred_data,w,l)
                # para_mape = [p[0],p[1],mape]
                # para_list = list(para_mape)
                # mape_result.append(para_mape)
                # print(final_data)
                pred.append(final_data)
    print(mape_result)

    pred.append(final_data)
    GMV = pd.concat(pred)
    # GMV = pd.DataFrame(mape_result,columns=['n_value','h_value','mape'])
    print(GMV)
    # final = GMV.to_excel('mjyx1.xls', sheet_name='pred')
    one = GMV.to_excel('mape.xls', sheet_name='pred')