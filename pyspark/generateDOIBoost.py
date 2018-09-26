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


def merge_author_info(a1, a2):    
    add_info = False
    if a1['identifiers'] is None:
        a1['identifiers'] = a2['identifiers']
        add_info = True
    elif a2['identifiers'] is not None:
        a1['identifiers'] += a2['identifiers']
        add_info = True
    if a1['affiliations'] is None:
        a1['affiliations'] = a2['affiliations']
        add_info = True
    elif a2['affiliations'] is not None:
        a1['affiliations'] += a2['affiliations']
        add_info = True    
    
    return (a1,add_info)

def convert_record(x):
    result = x.asDict(recursive=True)
    tmp ={}
    for item in result:
        if '_' not in item:
            tmp[item] = result[item]
    
    tmp['collectedFrom'] = ['CrossRef']
    added_Mag = False
    added_unpayWall = False
    added_orcid= False
    if result['abstract_mag'] is not None:
        for item in result['abstract_mag']:
            item['provenance'] = 'MAG'
            item.pop('provenanve')
            tmp['abstract'].append(item)
        added_Mag = True
    
    if result['author_mag'] is not None and len(result['author_mag'])> 0:
        a_mag = extract_author(result['author_mag'])
        a_cross= extract_author(result['authors'])
        similarities = matchAuthors( a_cross,a_mag, .65)
        if similarities is not None:
            for item in similarities:
                r = merge_author_info(tmp['authors'][item[0]], result['author_mag'][item[1]])
                tmp['authors'][item[0]] = r[0]        
                added_Mag = added_Mag or r[1]

    if result['authors_orcid'] is not None and len(result['authors_orcid'])> 0:
        a_mag = extract_author(result['authors_orcid'])
        a_cross= extract_author(result['authors'])
        similarities = matchAuthors( a_cross,a_mag, .65)
        if similarities is not None:
            for item in similarities:
                r = merge_author_info(tmp['authors'][item[0]], result['authors_orcid'][item[1]])             
                tmp['authors'][item[0]] = r[0]
                added_orcid = added_Mag or r[1]

    if result['license_uw'] is not None:
        added_unpayWall = True
        for item in result['license_uw']:
            tmp['license'].append(item)

    if added_Mag:
        tmp['collectedFrom'].append('MAG')
    if added_orcid:
        tmp['collectedFrom'].append('ORCID')  
    if added_unpayWall:
        tmp['collectedFrom'].append('UnpayWall')  
    return tmp



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


    tj.rdd.map(convert_record).saveAsTextFile(path='/data/df/doiBoost',compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
