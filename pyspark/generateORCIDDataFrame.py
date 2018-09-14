from pyspark import SparkContext
from pyspark.sql import SparkSession
import json


def map_ORCID(x):
    result = []
    author = dict(fullname='', identifiers = [dict(scheme='ORCID', value="https://orcid.org/"+x['orcid'], provenance='ORCID')], affiliations=[], given= x.get('firstname',''), family=x.get('lastname',''))
    fullname= "%s %s"%(author['given'], author['family'])
    author['fullname'] = fullname.strip()
    for item in x['publications']:
        if item['doi'] is not None and len(item['doi'])> 0:
            result.append((item['doi'].lower(), [author]))
    return result
    

if __name__ == '__main__':
    sc = SparkContext(appName='generateORCIDDataFrame')
    sc.textFile('/data/orciddump.txt').map(json.loads).flatMap(map_ORCID).reduceByKey(lambda a, b : a+b).map(lambda x: dict(doi=x[0], authors= x[1])).saveAsTextFile(path="/data/ORCID_df",compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
    
