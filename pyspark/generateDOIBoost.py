from pyspark import SparkContext


def levenshtein(s1, s2):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1 # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]

def similarity(a, b):
  if len(a)== 0 and len(b)==0:
    return 0
  c = max(len(a), len(b))
  return float((c - levenshtein(a,b) ) / float(c))





if __name__=='__main__':
    sc = SparkContext(appName='generateDOIBoost')

    
    mag = sc.textFile('/data/microsoft_df').map(eval).map(lambda x: (x['doi'].lower(), x))

    crossref = sc.textFile('/data/crossRef_df').map(eval).map(lambda x: (x['doi'].lower(), x))


    #join CrossRef with Mag
    join = mag + crossref

    print join.groupByKey().filter(lambda x: len(list(x[1])) > 1).count()
