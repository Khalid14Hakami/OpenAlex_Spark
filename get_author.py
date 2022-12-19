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
#institutions_list = get_file_url("/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/institutions/manifest", "/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/")
#venues_list = get_file_url("/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/venues/manifest", "/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/")
schema = StructType([
    StructField("id", StringType(), False),
    StructField("publication_year", IntegerType(), False),
    StructField(
        "host_venue",
        StructType([
             StructField("id", StringType(), False),
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
    venue =  x['host_venue']['id'] if  x['host_venue']['id'] != None else 'none1'
    if len( x['authorships']) > 0 and venue != 'none1':
        for i in x['authorships']:
            if len(x['id']) > 1:
                results.append( (x['id'], x['publication_year'], venue, i['author']['id']) )
    #else:
     #   results.append( (x['id'], x['publication_year'], venue, 'none2') )
    return results

for i in range(0, length, step):
    print("$$$$$$$$$$$$$$$$$$$$$$$$            $$$$$$$$$$$$$$$$$$$$4               $$$$$$$$$$$$$")
    print(i)
    print(i+step)
    print(length)
    end = i+step
    if end> length:
        end = length-1
    print(works_list[i:end])
    start_time = time.time()
    works= spark.read.schema(schema).\
        	json(works_list[i:end], primitivesAsString='true')
    		#json('/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/works/updated_date=2022-10-05/part_000', primitivesAsString='true') #7.4G        
		#load(works_list[i:end])
 	#	load('/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/works/updated_date\=2022-02-22/part_000') # meduim file		
		#load('/ibex/scratch/projects/c2194/hakamika/openalex-snapshot/data/works/updated_date=2022-04-29/part_000') # no author file
    end_time = time.time()
    print('Laoding Time: ', end_time-start_time)
    start_time = time.time()
    # query = works.select(['id', 'publication_year', 'host_venue.url',  explode('authorships.author.id')]).toDF('work_id', 'publication_year', 'venue_id', 'author_id')
    works.filter(col("author") == "") \
    .show(truncate=False) 
    rdd = works.rdd.flatMap(transform)
    df2=rdd.toDF(['work_id', 'publication_year', 'venue_id', 'author'])
    df2.write.csv("/ibex/scratch/projects/c2194/hakamika/testing/results_flatten_demo/"+str(i), header=True)
    end_time = time.time()
    print('Writing Time: ', end_time-start_time)    

