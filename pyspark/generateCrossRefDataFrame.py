from pyspark import SparkContext
from pyspark.sql import SparkSession



def generate_crossRefBoost(x):
    #Initializing Object
    result = dict(title="", authors =[], issued=x.get('issued', ''), abstract= [],subject=x.get('subject',[]),type =x.get('type',''), license=x.get('license',[]),instances=[], accepted=x.get('accepted',''),publihser=x.get('publisher'), doi=x['DOI'], issn=[], collectedFom =['CrossRef'])
    result['doi_url'] = "http://dx.doi.org/" + x['DOI']   
    result["published-online"] =x.get('published-online')
    result["published-print"] = x.get('published-print')
        
    # Adding Title
    if 'title' in x and x['title'] is not None and  len(x['title']) > 0 :
        result['title'] = x['title']
    
    if "issn-type" in x and x["issn-type"] is not None and len(x["issn-type"]) > 0:
        result['issn'] = x["issn-type"]

    for item in x.get('author',[]):
        at = dict(given= item['given'], family =item['family'])
        if len(item.get('ORCID','')) > 0:
            at['identifiers'] = [dict(scheme='ORCID', value=item['ORCID'], provenance='CrossRef')]
        if len(item.get('affiliation',[])) > 0:
            at['affiliations'] = [dict(value=aff['name'], identifiers =[], provenance='CrossRef') for aff in item['affiliation']]
        result['authors'].append(at)    
    if 'abstract' in x and x['abstract'] is not None and len(x['abstract']) > 0: 
        result['abstract'].append({  "value":x['abstract'], "provenance":"CrossRef" })

    if 'link' in x and x['link'] is not None and len(x['link']) > 0: 
        result['instances'] = [{  "url":cr_instance['URL'], "access-rights":"UNKNOWN", "provenance":"CrossRef" } for cr_instance in x['link'] ]
    return result
    



if __name__ == '__main__':
    pass