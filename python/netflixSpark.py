'''
Created on Dec 24, 2019

@author: pgonzalo
'''

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

import time
import datetime
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


sc = SparkContext()
spark = SparkSession(sc)


type_dict = {'Movie': 1, 'TV Show': 2}
rating_dict = {'Shavidee Trotter': 1,
    'Adriane Lenox': 2,
    '40 min': 3, 
    'TV-Y': 4,
    'UR': 5, 
    'null': 6,
    'PG': 7, 
    'TV-MA': 8,
   'Jowharah Jones': 9,
    'TV-Y7-FV': 10,
    'NR': 11,
    'TV-PG': 12,
    'NC-17': 13,
   'Richard Pepple': 14,
    'R': 15,
    'G': 16,
    'TV-14': 17, 
    'TV-G': 18,
    'TV-Y7': 19,
    'PG-13': 20,
    'Heather McDonald': 21, 
    'Rachel Dratch': 22}



def dateToUnix(old_date):
    #transform each date to UNIX format
    if old_date == "" or old_date == " " or old_date == None:
        return 0.0
    da = old_date.split(",")
    year = da[1].replace(" ", '')
    da = da[0].split(" ")
    day = da[1]
    month = da[0]
    date_new = year+"/"+month+"/"+day
    final_date = time.strptime(date_new, "%Y/%B/%d")
    d = datetime.date(final_date.tm_year, final_date.tm_mon, final_date.tm_mday)
    return time.mktime(d.timetuple())
def preprocess(sent):
    #preprocess the data and give values to  null values
    date_added = dateToUnix(sent.date_added)
    rating = -1
    try:
        rating = rating_dict[sent.rating]
    except:
        ''
    type_n = -1
    try:
        type_n = type_dict[sent.type]
    except:
        ''
    cast_arr = []
    if not sent.cast == None:
        cast_arr = sent.cast.split(",")
    director = 'nulo'
    if not sent.director == None:
        director = sent.director
    country = 'nulo'
    if not sent.country == None:
        country = sent.country
    genre = 'nulo'
    if not sent.listed_in == None:
        genre = sent.listed_in
    new_row = [sent.show_id, director, cast_arr, 
               country, date_added, sent.release_year, rating, type_n, genre]
    return new_row




def group_def(val):
    #group value by key (key, value)
    new_row = [val.show_id, val.new_director, val.NUMBER_CAST, 
               val.new_country, val.date_added, val.release_year, val.rating, val.type, val.new_genre]
    new_val = (val.show_id, new_row)
    return new_val


def last_format(x):
    #join the values with the same key
    cast = []
    list_rows = list(x[1])
    for row in list_rows:
        cast.append(row[2])
    row_return = list_rows[0]
    row_return[2] = cast
    return row_return

def calculate_WSS(kmax, training):
    sse = []
    
    
    for k in range(2, kmax):
        
        kmeans = KMeans().setK(k).setSeed(1)
        model = kmeans.fit(training)
        
        centroids = model.clusterCenters()
        
        transformed = model.transform(training).select("features","prediction")
        curr_sse = 0
        train_col = training.collect()
        trans_col = transformed.collect()
        for i in range(len(train_col)):
            curr_center = centroids[trans_col[i].prediction]
            val = 0.0
            for cont_fet in range(len(train_col[i].features)):
                val += (train_col[i].features[cont_fet] - curr_center[cont_fet]) ** 2
            curr_sse += val
                
        sse.append(curr_sse)
    return sse

#read the values and preprocess it
df = spark.read.load("dataset/netflix_2019.csv", format="csv", sep=",", header="True", emptyValue='')
counts = df.rdd.map(lambda line: preprocess(line)).toDF()


counts = counts.withColumn('show_id', counts["_1"].cast("string"))
counts = counts.withColumn('director', counts["_2"].cast("string"))
counts = counts.withColumn('cast', counts["_3"].cast("array<string>"))
counts = counts.withColumn('country', counts["_4"].cast("string"))
counts = counts.withColumn('date_added', counts["_5"].cast("float"))
counts = counts.withColumn('release_year', counts["_6"].cast("int"))
counts = counts.withColumn('rating', counts["_7"].cast("int"))
counts = counts.withColumn('type', counts["_8"].cast("int"))
counts = counts.withColumn('genre', counts["_9"].cast("string"))

#remove old columns
columns_del = ["_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", '_9']
counts = counts.drop(*columns_del)

from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

nd = counts.select(counts['director'], counts['country'], counts['genre'])
indexer = [StringIndexer(inputCol=column, outputCol=column+"_index") if not(column == 'cast') else column for column in list(set(nd.columns)-set(['overall']))]

#crete a new pipeline to get the proper int for each director, country and genre values
pipeline = Pipeline(stages=indexer)
transformed = pipeline.fit(nd).transform(nd)
transformed.show()
transformed.createOrReplaceTempView("TRANSFORMED_TEMP")

counts.createOrReplaceTempView("TEMP_DF")

#here we can take every actor from each array column and assign a new int value
row = spark.sql("SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS NUMBER_CAST, explode(cast) as cast, show_id FROM TEMP_DF")
row.createOrReplaceTempView("CAST_TEMP")


#generate a query to transform every string value to the proper int
new_row = spark.sql("SELECT ROW_NUMBER() OVER (PARTITION BY temp_df.show_id ORDER BY temp_df.show_id) as row_delete, * , director_index AS new_director, \
    country_index as new_country, genre_index as new_genre \
    FROM TEMP_DF INNER JOIN TRANSFORMED_TEMP ON TRANSFORMED_TEMP.director = TEMP_DF.director\
     AND TRANSFORMED_TEMP.country = TEMP_DF.country \
     AND TRANSFORMED_TEMP.genre = TEMP_DF.genre \
      INNER JOIN CAST_TEMP ON TEMP_DF.show_id=CAST_TEMP.show_id")


#remove the old values
new_row = new_row.drop(*['director', 'director_index', 'country_index', 'genre_index', 'country', 'cast'])

#group all the values by the key show id
end = new_row.rdd.map(lambda x: group_def(x)) \
        .groupByKey() \
        .map(lambda x: last_format(x)).toDF()

#rename the values
end = end.withColumn('show_id', end["_1"].cast("string"))
end = end.withColumn('director', end["_2"].cast("float"))
end = end.withColumn('casting', end["_3"].cast("array<float>"))
end = end.withColumn('country', end["_4"].cast("float"))
end = end.withColumn('date_added', end["_5"].cast("float"))
end = end.withColumn('release_year', end["_6"].cast("float"))
end = end.withColumn('rating', end["_7"].cast("float"))
end = end.withColumn('type', end["_8"].cast("float"))
end = end.withColumn('genre', end["_9"].cast("float"))

#delete the old ones
end = end.drop(*columns_del)
end.show(20)

#get the values to create the cluster
vecAssembler = VectorAssembler(inputCols=['director', 'country', 'release_year', 'rating', 'type', 'genre'], outputCol="features")
vector_df = vecAssembler.transform(end)
#split training and test
(training, test) = vector_df.randomSplit([0.8, 0.2])

#to calculate the ideal value of k
#sse = calculate_WSS(60, training)
#adecuate k between 6 and 8 in this case

kmeans = KMeans().setK(8).setSeed(1)
model = kmeans.fit(training)

centroids = model.clusterCenters()

#so given a new value about a film you can predict which cluster it belongs to
# and so that similar TV or series that belongs to the same cluster
transformed = model.transform(test).select("features","prediction")
transformed.show(30)