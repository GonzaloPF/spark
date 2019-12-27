'''
Created on Dec 25, 2019

@author: pgonzalo
'''


from __future__ import print_function


from pyspark.context import SparkContext
import numpy as np
from pyspark.sql.session import SparkSession

borough = {"City of London": 1,
    "Brent": 2,
    "Barnet": 3,
    "Bexley": 4,
    "Camden": 5,
    "Ealing": 6,
    "Harrow": 7,
    "Merton": 8,
    "Newham": 9,
    "Sutton": 10,
    "Bromley": 11,
    "Croydon": 12,
    "Enfield": 13,
    "Hackney": 14,
    "Lambeth": 15,
    "Haringey": 16,
    "Havering": 17,
    "Hounslow": 18,
    "Lewisham": 19,
    "Greenwich": 20,
    "Islington": 21,
    "Redbridge": 22,
    "Southwark": 23,
    "Hillingdon": 24,
    "Wandsworth": 25,
    "Westminster": 26,
    "Tower Hamlets": 27,
    "Waltham Forest": 28,
    "Barking and Dagenham": 29,
    "Kingston upon Thames": 30,
    "Richmond upon Thames": 31,
    "Hammersmith and Fulham": 32,
    "Kensington and Chelsea": 33}
def parseVector(line):
    #obtain the columns desired
    vect = line.split(',')[1:]
    vect = vect[0:1] + vect[3:]
    #format each city with the properly number of the dictionary
    vect_fin = [float(x) if x.isdigit() else float(borough.get(x)) for x in vect ]
    
    return np.array(vect_fin)

def closestPoint(p, centers):
    bestIndex = 0
    
    closest = float("+inf")
    
    for i in range(len(centers)):
        
        tempDist = np.sum((p - centers[i]) ** 2)
        
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
        
    return bestIndex



if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sc)
    #Google dataset public (bigquery-public-data:london_crime.crime_by_lsoa)
    lines = sc.textFile('../files/crime*')
    
    #removing header
    tagsheader = lines.first()
    header = sc.parallelize([tagsheader])
    lines = lines.subtract(header)
    
   
    data = lines.map(parseVector).cache()
    #set the number of clusters
    k = 10
    #set convergence parameter
    convergeDist = 0.1
    
    #get the first centroid in a randome way
    refPoints = data.takeSample(False, k, 1)
    tempDist = 1.0
    
    while tempDist > convergeDist:
        #find the closer centroid to each reg
        closest = data.map(lambda p: (closestPoint(p, refPoints), (p,1)))
        #sum the total number of elements in each centroid
        pointStats = closest.reduceByKey(lambda p1_c1, p2_c2: (p1_c1[0] + p2_c2[0], p1_c1[1] + p2_c2[1]))
        #calculate the new centroid by dividing the total sum of centroids by total elements in it
        newPoints = pointStats.map(lambda st: (st[0], st[1][0] / st[1][1])).collect()
        #calculate the tempdist to see the convergence
        tempDist = sum(np.sum((refPoints[iK] - p) **2) for (iK, p) in newPoints)
        #set the new centroid in the KPoints variable
        for (iK, p) in newPoints:
            refPoints[iK] = p
    
    closest = closest.map(lambda x: str(x[0]) + "," + str(x[1][0][0]) + "," + str(x[1][0][1]) + "," + str(x[1][0][2]) + "," + str(x[1][0][3])) 
    closest.saveAsTextFile("CLUSTER") 
    
    print("Final centers: " + str(refPoints))
    #read the files and tranform it to dataframe
    df = spark.read.format("csv").option("header", "false").load("./CLUSTER/part*")
    #format the new data as float
    df = df.withColumn('GROUP', df["_c0"].cast("float"))
    df = df.withColumn('CITY', df["_c1"].cast("float"))
    df = df.withColumn('YEAR', df["_c3"].cast("float"))
    df = df.withColumn('MONTH', df["_c4"].cast("float"))
    df = df.withColumn('VALUE', df["_c2"].cast("float"))
    #remove old columns
    columns_del = ["_c0", "_c1", "_c2", "_c3", "_c4"]
    df = df.drop(*columns_del)
    #create a temporary view with the data
    df.createOrReplaceTempView("TEMP_DF")
    
    #find the period of time with more crime in the city 26 = Westminster
    row = spark.sql("SELECT GROUP, CITY, SUM(VALUE) as TOT_VALUE, MONTH, COUNT(MONTH) as C_MONTH FROM TEMP_DF WHERE  CITY=26 GROUP BY GROUP, MONTH, CITY ORDER BY TOT_VALUE DESC LIMIT 10")
    #show the result
    row.show(150)
    #the result show us that the crime, for any reason, increase in the second middle of the year (cluster 6)
    #and is smaller in the first middle (cluster 5)
    spark.stop()

    
