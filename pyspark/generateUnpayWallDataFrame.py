import csv
from pyspark import SparkContext


def generate_line(line):
    result =[]
    for l in  csv.reader(line, quotechar='"', delimiter=',',
                          quoting=csv.QUOTE_ALL, skipinitialspace=True):
        result.append(l)
    return result


def generate_record(x):
    return dict(doi=x[0], instances=[{  "url":x[3],  "access-rights":"OPEN", "provenance":"UnpayWall" }])

if __name__ == '__main__':
    sc = SparkContext(appName='generateUnPayWallDataFrame')
    sc.textFile('/data/oa_doi.csv').flatMap(lambda x: generate_line([x.encode('utf-8')])).filter(lambda x: len(x) == 14 and x[1]=='t').map(generate_record).saveAsTextFile(path="/data/unpaywall_df",compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

