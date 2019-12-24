'''
Created on Dec 24, 2019

@author: pgonzalo
'''

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

lemma = WordNetLemmatizer()
sc = SparkContext()
spark = SparkSession(sc)
stop_words = set(stopwords.words('english'))

def preprocess(sent):
    #remove digits from text line
    sent = ''.join([i for i in sent if not i.isdigit()])
    #tokenize each word
    sent = nltk.word_tokenize(sent, 'english')
    sent = nltk.pos_tag(sent)
    #clean the words and remove useless
    new_sent = [word for word in sent if not clean(word) == None]
    #return the new line list
    return new_sent



def lem(word):
    #find each representation of each word
    #and put the correct value n: name, v: verb, a: adjective, r: adverb
    w = 'n'
    if word[1][0] == 'V':
        w = 'v'
    elif word[1][0] == 'J':
        w = 'a'
    elif word[1][0] == 'A':
        w = 'r' 
    
    #lemmatize the word for the appropriate entity
    return lemma.lemmatize(word[0], pos=w).encode('utf-8')

def clean(word):
    #clean the word, removing stop and short words
    if len(word[0].encode('utf-8')) < 4:
        return None
    else:
        if not word[0].lower().encode('utf-8') in stop_words:
            new_word = (word[0].encode('utf-8'), word[1])
            return new_word
    return None



#insert a new English text to be processed
counts = sc.textFile('big.txt').flatMap(lambda line: preprocess(line)) \
    .map(lambda word: (lem(word), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda a: a[0] + " , " + str(a[1]))
    
#save the result as text files
counts.saveAsTextFile('exit')

#read the files and tranform it to dataframe
df = spark.read.format("csv").option("header", "false").load("./exit/part*")
#create a temporary view with the data
df.createOrReplaceTempView("TEMP_DF")
#find the 10 most used words
row = spark.sql("SELECT temp_df._c0 as WORD, temp_df._c1 as COUNT FROM TEMP_DF ORDER BY COUNT DESC limit 10")
#show the result
row.show()