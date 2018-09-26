from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct,length, first, array, lit
from difflib import SequenceMatcher

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def max_in_matrix(m, t):
    m_values = []
    for row in m:
        m_values.append((max(row), row.index(max(row))))

    mx =max(m_values)
    if (mx[0]) < t:
        return None
    return (m_values.index(mx),mx[1])



def matchAuthors(a, b, t):
    row = len(a)
    col = len(b)
    matrix =[]
    for r in range(row):
        current_row = []
        for c in range(col):
            a1 = a[r]
            a2 = b[c]
            current_row.append(similarity(a1,a2))
        matrix.append(current_row)
    
    mx = max_in_matrix(matrix, t)
    similarities = []
    while mx is not None:
        similarities.append(mx)
        matrix[mx[0]] = [0] * col
        for item in matrix:
            item[mx[1]] = 0
        mx = max_in_matrix(matrix, t)
    
    return similarities
    


def extract_author(authors):
    res = []
    for item in authors:
        if item['fullname'] is not None and len(item['fullname']) > 0:
            res.append(item['fullname'])
        elif item['given'] is not None or item['family'] is not None:
            res.append("%s %s"%(item['given'],item['family'] ))
    return res


if __name__=='__main__':
    sc = SparkContext(appName='generateDOIBoost')
    spark = SparkSession(sc)

    #Loading CrossRef Dataframe
    crossref = spark.read.load('/data/df/crossref.parquet', format="parquet")
    
    #Loading MAG Dataframe
    microsoft = spark.read.load("/data/df/mag.parquet", format="parquet")

    #Alias each column with _mag
    microsoft = microsoft.select(*(col(x).alias(x + '_mag') for x in microsoft.columns))    

    #Group By DOI since we have repeated doi with multiple abstract, at the moment we take the first One
    mag = microsoft.groupBy('doi_mag').agg(first('authors_mag').alias('author_mag'), first('abstract_mag').alias('abstract_mag'),first('collectedFom_mag').alias('collectedFrom_mag') )
    
    #Load ORCID DataFrame
    orcid = spark.read.load("/data/df/ORCID.parquet",format="parquet")
  
    #Fix missing value in collectedFrom
    orcid =orcid.withColumn('collectedFrom',array(lit('ORCID')))

    #Alias each column with _orchid
    orcid = orcid.select(*(col(x).alias(x + '_orcid') for x in orcid.columns))


    #Load UnpayWall DataFrame
    uw= spark.read.load("/data/df/unpaywall.parquet",format="parquet")
    
    #Alias each column with _uw
    uw =uw.select(*(col(x).alias(x + '_uw') for x in uw.columns))

    #Fix missing value in collectedFrom
    uw =uw.withColumn('collectedFrom',array(lit('UnpayWall')))


    #Implement first join between crossref and MAG
    fj = crossref.join(mag, crossref.doi == mag.doi_mag, how="left")

    # Join the result of the previous Join with ORCID
    sj = fj.join(orcid, fj.doi== orcid.doi_orcid, how='left')

    # Join the result of the previous Join with UnpayWall
    tj = sj.join(uw, sj.doi== uw.doi_uw, how='left')


    print tj.count()
