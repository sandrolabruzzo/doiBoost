#!sh

perl doiFix.pl    sample-20.txt > sample-20.ndjson
perl doiFix.pl -j sample-20.txt > sample-20.json
# jsonpp < sample-20.json > sample-20-pp.json
perl doiFix.pl -l sample-20.txt > sample-20.jsonld
riot --formatted=turtle sample-20.jsonld > sample-20.ttl
riot --validate sample-20.ttl
