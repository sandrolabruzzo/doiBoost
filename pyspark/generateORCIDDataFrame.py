from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct,length
from pyspark.sql.types import *
import re

regex = r"\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\'])\S)+)\b"

def fix_doi(x):
    matches = re.search(regex, x[1])
    if matches:
        return dict(orcid=x[0].strip(),doi = matches.group(0))
    return None


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


def map_item(x):
    authors = {}
    for item in x[1]:
        authors[item['orcid_w']] =dict(fullname='%s %s'%(item['firstname'], item['lastname']), identifiers = [dict(scheme='ORCID', value="https://orcid.org/"+item['orcid_w'], provenance='ORCID')], affiliations=[], given= item['firstname'], family=item['lastname'])
    return dict(doi=x[0].lower(), authors=authors.values())    

   

if __name__ == '__main__':
    sc = SparkContext(appName='generateORCIDDataFrame')    
    spark = SparkSession(sc)


    #create dataFrame for ORCID WORKS a set of (ORCID, DOI) and save in hdfs
    sc.textFile('/tmp/orcid_works').map(eval).map(fix_doi).filter(lambda x: x is not None).toDF().write.save('/tmp/works_orcid.parquet', format='parquet')

    #create dataFrame for ORCID People info: a set of (ORCID, firstname, lastname) and save in hdfs
    sc.textFile('/tmp/person_orcid').map(eval).map(lambda x: dict(orcid=x[0],firstname=x[1], lastname=x[2])).toDF().write.save('/tmp/person_orcid.parquet', format='parquet')

    p_df = spark.read.load('/tmp/person_orcid.parquet', format='parquet')

    w_df = spark.read.load('/tmp/works_orcid.parquet', format='parquet')

    w_df = w_df.select(*(col(x).alias(x + '_w') for x in w_df.columns))

    p_w_join = p_df.join(w_df, p_df.orcid ==w_df.orcid_w).select(w_df.orcid_w, w_df.doi_w, p_df.firstname, p_df.lastname)

    aggregation = p_w_join.groupBy('doi_w').agg(collect_list(struct('orcid_w', 'firstname', 'lastname')))

    aggregation.rdd.map(map_item).toDF(get_schema()).write.save("/data/ORCID.parquet", format="parquet")
    
    
    
