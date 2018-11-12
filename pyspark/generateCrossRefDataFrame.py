from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json

def convert_date(x):
    if type(x)== dict and 'date-parts' in x:
        item = x['date-parts']
        if type(item)== list and len(item)>0:
            date_parts = item[0]            
            if date_parts is not None and type(date_parts) == list:
                if date_parts[0] is None:
                    return None
                if len(date_parts)<3:                    
                    while len(date_parts)!=3:                        
                        date_parts.append(1)
                return"-".join([str(k) for k in date_parts])
    return None

def get_first(x):
    for item in x[1]:
        return item

def generate_crossRefBoost(x):
    #Initializing Object
    result = dict(title=[], authors =[], issued=convert_date(x.get('issued', '')), abstract= [],subject=x.get('subject',[]),type =x.get('type',''), license=[],instances=[], accepted=convert_date(x.get('accepted','')),publisher=x.get('publisher'), doi=x['DOI'], issn=[], collectedFrom =['CrossRef'])
    result['doi-url'] = "http://dx.doi.org/" + x['DOI']   
    result["published-online"] =convert_date(x.get('published-online'))
    result["published-print"] = convert_date(x.get('published-print'))
    
        
    # Adding Title
    if 'title' in x and x['title'] is not None and  len(x['title']) > 0 :
        result['title'] = x['title']
    
    if "issn-type" in x and x["issn-type"] is not None and len(x["issn-type"]) > 0:
        result['issn'] = x["issn-type"]

    for item in x.get('author',[]):
        at = dict(given= item.get('given'), family =item.get('family'))
        at['fullname'] = "%s %s"%(at['given'], at['family'])
        if len(item.get('ORCID','')) > 0:
            at['identifiers'] = [dict(scheme='ORCID', value=item['ORCID'], provenance='CrossRef')]
        if len(item.get('affiliation',[])) > 0:
            at['affiliations'] = [dict(value=aff['name'], identifiers =[], provenance='CrossRef') for aff in item['affiliation']]
        result['authors'].append(at)    
    if 'abstract' in x and x['abstract'] is not None and len(x['abstract']) > 0: 
        result['abstract'].append({  "value":x['abstract'], "provenance":"CrossRef" })

    if 'link' in x and x['link'] is not None and len(x['link']) > 0: 
        result['instances'] = [{  "url":cr_instance['URL'], "access-rights":"UNKNOWN", "provenance":"CrossRef" } for cr_instance in x['link'] ]
    
    if 'license' in x and type(x['license']) ==list: 
        print x['license']       
        for l in x['license']:            
            current_license = {}
            if 'URL' in l:
                current_license['url'] = l['URL'] 
            if 'url' in l:
                current_license['url'] = l['url'] 
            if 'start' in l:    
                current_license ['date-time'] =l['start']['date-time']
            if 'date-time' in l:
                current_license ['date-time'] =l['date-time']
            current_license['content-version'] = l['content-version']
            current_license['delay-in-days'] = l['delay-in-days']
            if 'url' in current_license:
                result['license'].append(current_license)
    return result
    

if __name__ == '__main__':

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

    sc = SparkContext(appName='generateCrossRefDataFrame')
    spark = SparkSession(sc)
    sc.textFile('/data/crossref_2018_11').map(json.loads).map(lambda x: (x['_source']['DOI'],generate_crossRefBoost(x['_source'])).groupByKey().map(get_first).toDF(schemaType).write.save("/data/crossref_2018_11.parquet", format="parquet")