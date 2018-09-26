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


def create_crossRef_record(x):
    result = dict(doi= x['doi'], authors=[], abstract=[], subject=[], license=[],instances=[],issn=[],collectedFrom=['CrossRef'])
    result['doi-url']= "http://dx.doi.org/%s"%result['doi']
    for abstract in x['abstract']:
        result['abstract'].append(dict(value= abstract['value'], provenance='CrossRef'))
    for subject in x['subject']:
        result['subject'].append(subject)
    
    


    
    


def transformRecord(x):
    if 'doi_mag' not in x or x['doi_mag'] is None:
        return create_crossRef_record(x)
