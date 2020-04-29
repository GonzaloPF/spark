'''
Created on Dec 24, 2019

@author: pgonzalo
'''

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
import numpy as np
import pandas as pd
import time
import datetime

sc = SparkContext()
spark = SparkSession(sc)


def dateToUnix(old_date):
    #transform each date to UNIX format
    if old_date == "" or old_date == " " or old_date == None:
        return 0.0
    da = old_date.split("-")
    year = da[0].replace(" ", '')
    day = da[2]
    month = da[1]
    date_new = year+"/"+month+"/"+day
    final_date = time.strptime(date_new, "%Y/%m/%d")
    d = datetime.date(final_date.tm_year, final_date.tm_mon, final_date.tm_mday)
    return time.mktime(d.timetuple())
#group by values
def groupByProv(val):
    return (val.cod_ine,[val.fecha,val.total])
def groupByProv_now(val):
    return (val.cod_ine,val.total)

#remove total CCAA = 00
def filterTotal(x):
    if str(x[0]) == "00":
        return False
    return True


def linearReg(x):
    #calculate linear regression by CCAA given a date
    data = list(x[1])
    code = []
    date = []
    casos = []
    for item in data:
        code.append(x[0])
        date.append(float(dateToUnix(item[0])))
        casos.append(float(item[1]))
    #create dataset
    dataset = {'code': code,
            'date': date,
            'casos': casos}
    #transform to dataframe
    data = pd.DataFrame(data=dataset)
    x_pred = data.date
    y_pred = data.casos
    #create the model with the values
    model = np.polyfit(x_pred, y_pred, 1)
    predict = np.poly1d(model)
    new_date = dateToUnix("2020-06-25") #date you want to predict
    prediction = predict(new_date)
    #return the value
    return (x[0], float(prediction))
      
def greaterThan(x,y):
    #get the greater value
    if int(x) > int(y):
        return x
    return y


#read the amount of cases
df_casos = spark.read.load("covid19casos.csv", format="csv", sep=",", header="True", emptyValue='')

#RDD with the current values: get the greater value by CCAA
end_casos_now = df_casos.rdd.map(lambda x: groupByProv_now(x)) \
                        .filter(lambda x: filterTotal(x)) \
                        .reduceByKey(lambda x,y: greaterThan(x,y)) \
                        .map(lambda x: (x[0],x[1]))
#RDD make predictions
end_casos = df_casos.rdd.map(lambda x: groupByProv(x)) \
                        .filter(lambda x: filterTotal(x)) \
                        .groupByKey().map(lambda x: linearReg(x))


#join current values with prediction ones
end = end_casos.join(end_casos_now).map(lambda x: (x[0],x[1][0],x[1][1])).toDF()

end = end.withColumn('cod_prov', end["_1"].cast("int")) 
end = end.withColumn('prediction', end["_2"].cast("float")) 
end = end.withColumn('last_day', end["_3"].cast("float"))
end = end.drop(*["_1","_2","_3"]) 
#show the values
end.show()
