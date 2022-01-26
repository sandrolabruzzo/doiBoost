The perl script `doiFix.pl` fixes DOIboost output problems as described in [#1](https://github.com/sandrolabruzzo/doiBoost/issues/1) (only `http://dx.doi.org` URLs are fixed) and removes empty/useless fields as per [#4](https://github.com/sandrolabruzzo/doiBoost/issues/4).

It has these options:
- `-j` converts to JSON by wrapping in array: `[line1, line2, ...]`
- `-l` converts to JSONLD by also prepending a context: `{"@context": {...}, "@graph": [line1, line2, ...]}`
- if neither is used, converts to NDJSON (newline-delimited JSON)
- `-d` normalize DOIs by lowercasing them, see https://www.doi.org/doi_handbook/2_Numbering.html#2.4

The script `doiFix-test.sh` exercises the perl script with the first 20 records of the original output. It also uses Jena RIOT to convert JSONLD to Turtle, also checking for validity
- `sample-20.txt` is the original Pythonic broken "JSON"
- `sample-20.ndjson` is NDJSON
- `sample-20.json` is JSON
- `sample-20.jsonld` is JSONLD
- `sample-20.ttl` is Turtle
