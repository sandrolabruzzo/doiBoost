from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct,length


def createAbstract(x):    
    if 'IndexedAbstract' in x:
        p = eval(x['IndexedAbstract'])
        if 'IndexLength' not in p:
            return None        
        result = [""]* p['IndexLength']
        for key, value in p['InvertedIndex'].iteritems():
            for item in value:                
                result[item] = key
        return dict(PaperID=x['PaperID'],abstract=" ".join(result))
    return None

def map_microsoft(x):
    result= dict(id= x[0], authors=[])
    result["collected-from"]=[ "MAG"]    
    for item in x[1]:
        result['doi'] = item['DOI_paper'].lower()
        if item['abstract'] is not None and len(item['abstract'])>0 and 'abstract' not in result:
            result['abstract']= [{  "value":item['abstract'], "provenance":"MAG" }]
        author = dict(fullname=item['DisplayName_author'], identifiers = [], affiliations=[])
        author['identifiers'].append(dict(schema= 'URL', value="https://academic.microsoft.com/#/detail/"+item['AuthorID']))
        current_affiliation =dict(value=item['DisplayName_affiliation'] , identifiers=[], provenance="MAG")
        current_affiliation['official-page'] = item['OfficialPage_affiliation']
        if item['WikiPage_affiliation'] is not None and len(item['WikiPage_affiliation']) > 0:
            current_affiliation['identifiers'].append(dict(schema="wikpedia",value=item['WikiPage_affiliation']))
        if item['GridID_affiliation'] is not None and  len(item['GridID_affiliation']) > 0:
            current_affiliation['identifiers'].append(dict(schema="grid.ac",value=item['GridID_affiliation']))
        author['affiliations'].append(current_affiliation)
        result['authors'].append(author)
    return result


if __name__=='__main__':
    sc = SparkContext(appName='generateMAGDataFrame')
    spark = SparkSession(sc)
    
    # Create Paper Abstract Dataframe from DUMP of Microsoft PaperAbstractsInvertedIndex
    abstract = sc.textFile('/data/MicrosoftDump/PaperAbstractsInvertedIndex').map(eval).map(createAbstract).toDF()
    
    # Create Paper Dataframe from DUMP of Microsoft Paper
    paper = sc.textFile('/data/MicrosoftDump/MicrosoftPapers').map(eval).toDF()

    #Rename Column to avoid collision of name during the Join
    paper = paper.select(*(col(x).alias(x + '_paper') for x in paper.columns))

    #filter Paper with DOI Total number should be 74582104 and join them with abstract
    paper_with_DOI = paper.where(length(paper.DOI_paper) > 0).join(abstract, abstract.PaperID == paper.PaperId_paper, how='left')

    # Create affiliation DataFrame from Microsoft Affiliation dump
    affiliation = sc.textFile('/data/MicrosoftDump/Affiliations').map(eval).toDF()

    #Rename Column to avoid collision of name during the Join
    affiliation =affiliation.select(*(col(x).alias(x + '_affiliation') for x in affiliation.columns))

    # Create author DataFrame from Microsoft Authors dump
    author = sc.textFile('/data/MicrosoftDump/Authors').map(eval).toDF()

    #Rename Column to avoid collision of name during the Join
    author =author.select(*(col(x).alias(x + '_author') for x in author.columns))

    #creating DataFrame  from PaperAuthorAffiliations  Relation
    paper_author_affiliation =sc.textFile('/data/MicrosoftDump/PaperAuthorAffiliations').map(eval).toDF()

    # First Join paper with relation
    p_join = paper_with_DOI.join(paper_author_affiliation, paper_with_DOI.PaperId_paper == paper_author_affiliation.PaperID, how='left')


    # Next Join with Affiliations
    af_join = p_join.join(affiliation, affiliation.AffiliationID_affiliation == p_join.AffiliationID,how='left')

    # Next Join with Authors

    complete_join = af_join.join(author, author.AuthorID_author == af_join.AuthorID,how='left')

    #Grouping by Paper Id
    aggregation = complete_join.groupBy('PaperId_paper').agg(collect_list(struct('abstract', 'DOI_paper', 'AffiliationID', 'AuthorID', 'AuthorSequenceNumber', 'DisplayName_affiliation', 'GridID_affiliation', 'OfficialPage_affiliation',  'WikiPage_affiliation',  'DisplayName_author',  'NormalizedName_author')))

    aggregation.rdd.map(map_microsoft).saveAsTextFile(path="/data/microsoft_new",compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
