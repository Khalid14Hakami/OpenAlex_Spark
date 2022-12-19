from pyspark.sql.functions import explode, col, size
from pyspark.sql import SparkSession, SQLContext
import time
import json 
import sys
from pyspark.sql.types import *
reload(sys)
sys.setdefaultencoding('utf-8')

def get_file_url(file, prefix):
    list = []

    f = open (file, "r")
    
    # Reading from file
    data = json.loads(f.read())
    
    # Iterating through the json
    # list
    for i in data['entries']:
        list.append(prefix + i['url'][19:-3])
    
    # Closing file
    f.close()
    return list


spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()


works_list = get_file_url("/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/works/manifest", "/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/")

schema = StructType([
    StructField("id", StringType(), False),
    StructField("publication_year", IntegerType(), False),
    StructField(
        "host_venue",
        StructType([
            StructField("url", StringType(), False),
        ]),
        False
    ),
    StructField(
        "authorships",
        ArrayType(
        StructType([
            StructField("author", StructType([
                StructField("id", StringType(), False)
            ]), False),
        ])),
        False
    ),
    ])
step = 3
length = len(works_list)
def transform(x):
    results = []
    if len(x['authorships']) > 0:
        for i in x['authorships']:
            results.append( (x['id'], x['publication_year'], x['host_venue']['url'], i['author']['id']) )
        return results
    return [(x['id'], x['publication_year'], x['host_venue']['url'], 'none')]

for i in range(0, 1, step):
    print("$$$$$$$$$$$$$$$$$$$$$$$$            $$$$$$$$$$$$$$$$$$$$4               $$$$$$$$$$$$$")
    print(i)
    print(i+step)
    print(length)
    end = i+step
    if end> length:
        end = length-1
    print(works_list[i:end])
    # loading 
    start_time = time.time()
    works= spark.read.option("header",True).\
        	csv('results_flatten3/*/*.csv')
    end_time = time.time()
    print('Laoding Time: ', end_time-start_time)
    with open("results.log", "a") as f:
        f.write('Laoding Time: '+ str( end_time-start_time )+ '\n')
    start_time = time.time()
    works = works.groupBy(['author', 'venue_id', 'publication_year']).count()
    
    end_time = time.time()
    print('Query Time: ', end_time-start_time)
    with open("results.log", "a") as f:
        f.write('query Time: '+ str( end_time-start_time )+ '\n')
    start_time = time.time()
    works.show()
    end_time = time.time()
    print('showing Time: ', end_time-start_time)
    with open("results.log", "a") as f:
        f.write('showing time: '+ str( end_time-start_time ) + '\n' )
    # counting 
    start_time = time.time() 
    #total_rows = works.count()
    end_time = time.time()
    with open("results.log", "a") as f:
        #f.write('number of rows in cleaned flatten: '+ str( total_rows ) + '\n' )
        f.write('counting time cleaned flatten: '+ str( end_time-start_time ) + '\n' )
    
    # sorting: 
    start_time = time.time()
    #works = works.orderBy(['author', 'venue_id', 'publication_year'], ascending=[1, 1, 1]) 
    end_time = time.time()
    with open("results.log", "a") as f:
        f.write('sorting time: '+ str( end_time-start_time ) + '\n' )
    
    # writing results:
    start_time = time.time()
    works.na.drop().coalesce(1).write.csv("/ibex/scratch/projects/c2194/hakamika/testing/results_clean_ordered_demo" )
    end_time = time.time()
    print('writing Time: ', end_time-start_time)
    with open("results.log", "a") as f:
        f.write('writing time cleaned flatten: '+ str( end_time-start_time ) + '\n' )

