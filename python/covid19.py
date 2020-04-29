'''
Created on Dec 24, 2019

@author: pgonzalo
'''

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import desc


sc = SparkContext()
spark = SparkSession(sc)
#group by values
def groupByProv(val):
    return (val.cod_ine,val.total)
def groupByProvPop(val):
    return (val.cod_ine,val.total)

#remove total CCAA = 00
def filterTotal(x):
    if str(x[0]) == "00":
        return False
    return True

#get the proportion of cases by CCAA
def proportion(x):
    return (x[0],round(float(x[1][0])/float(x[1][1].replace(".","")),4))
    


#read the amount of cases
df_casos = spark.read.load("covid19casos.csv", format="csv", sep=",", header="True", emptyValue='')
#read the population per CCAA
df_ccaa = spark.read.load("CCAA.csv", format="csv", sep=",", header="True", emptyValue='')

#create RDD for cases
#get only cod_prov and cases
#remove the Total CCAA since it's not useful
#reduce by key, adding up all the cases by CCAA
#format the values
end_casos = df_casos.rdd.map(lambda x: groupByProv(x)) \
                        .filter(lambda x: filterTotal(x)) \
                        .reduceByKey(lambda x,y: int(x) + int(y)) \
                        .map(lambda x: (x[0],x[1]))
                        
#create RDD for population
#get only code and total by CCAA
end_population = df_ccaa.rdd.map(lambda x: groupByProvPop(x))

#we join the values in a new RDD by code and get the proportion: total_case/total_population
end = end_casos.join(end_population).map(lambda x: proportion(x)).toDF()
#sort the values
end= end.sort(col("_2").desc()) 
#show the values
end.show()

