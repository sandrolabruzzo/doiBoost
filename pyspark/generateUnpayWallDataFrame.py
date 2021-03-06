import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct,length
from pyspark.sql.types import *



def get_schema():
    title_field =StructField('title',ArrayType(StringType(),True),True)
    identifiers_type = StructType([StructField('schema',StringType(),True), StructField('value', StringType(), True),StructField('provenance', StringType(), True)])
    affiliation_identifier_type= StructType([StructField('schema',StringType(),True), StructField('value', StringType(), True)])
    affiliation_type = StructType([StructField('value',StringType(),True),StructField('official-page',StringType(),True), StructField('identifiers', ArrayType(affiliation_identifier_type, True)) ,StructField('provenance', StringType(), True)])
    author_field_type = StructType([StructField("given", StringType(), True),StructField("family", StringType(), True), StructField("fullname", StringType(), True), StructField('identifiers', ArrayType(identifiers_type, True)),  StructField('affiliations', ArrayType(affiliation_type, True))]) 
    abstract_type = StructType([StructField('value', StringType(), True),StructField('provenance', StringType(), True)]) 
    license_type = StructType([StructField('url', StringType(), True), StructField('date-time', StringType(), True), StructField('content-version', StringType(), True), StructField('"delay-in-days', IntegerType(), True)]) 
    instance_type = StructType([StructField('url', StringType(), True), StructField('access-rights', StringType(), True), StructField('provenance', StringType(), True)]) 
    issn_type= StructType([StructField('type', StringType(), True), StructField('value', StringType(), True)]) 
    schemaType = StructType([
        title_field,
        StructField('authors', ArrayType(author_field_type, True), True), 
        StructField('issued',StringType(),True),
        StructField('abstract', ArrayType(abstract_type, True), True), 
        StructField('subject', ArrayType(StringType(),True),True), 
        StructField('type',StringType(),True),                                
        StructField('license', ArrayType(license_type,True),True), 
        StructField('instances', ArrayType(instance_type,True),True), 
        StructField('published-online',StringType(),True),                     
        StructField('published-print',StringType(),True),                     
        StructField('accepted',StringType(),True),                     
        StructField('publisher',StringType(),True),      
        StructField('doi',StringType(),True),      
        StructField('doi-url',StringType(),True),    
        StructField('issn', ArrayType(issn_type,True),True), 
        StructField('collectedFrom', ArrayType(StringType(),True),True)])
    return schemaType


def try_decode(x):
    try:
        return json.loads(x)
    except: 
        return None    


def generate_record(x):
    return dict(doi=x['doi'], instances=[{  "url":x['best_oa_location']['url'], "access-rights":"OPEN", "provenance":"UnpayWall" }],collectedFrom=['UnpayWall'])

if __name__ == '__main__':
    sc = SparkContext(appName='generateUnPayWallDataFrame')
    spark = SparkSession(sc)
    sc.textFile('/data/doiboost/input/unpaywall_snapshot_2019-08-16').map(try_decode).filter(lambda x: x is not None and x['is_oa'] is True).map(generate_record).toDF(get_schema()).write.mode('overwrite').save("/data/doiboost/dataframe/unpaywall2019-08.parquet", format="parquet")

