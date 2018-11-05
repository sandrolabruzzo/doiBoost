#!perl -wp

use strict;
use Getopt::Std;
our $opt_j, # convert to JSON by wrapping in array: [line1, line2, ...]
our $opt_l; # convert to JSONLD by also prepending a context: {"@context": {...}, "@graph": [line1, line2, ...]}
our $opt_d; # normalize DOIs by lowercasing them, see https://www.doi.org/doi_handbook/2_Numbering.html#2.4

BEGIN {
  getopts("jld");
  $opt_j ||= $opt_l;        # JSONLD *is* JSON
  print <<'EOF' if $opt_l;  # JSONLD context
{
  "@context": {
    "@base": "https://github.com/sandrolabruzzo/doiBoost/data/",
    "@vocab": "https://github.com/sandrolabruzzo/doiBoost/ontology/",
    "doib": "https://github.com/sandrolabruzzo/doiBoost/ontology/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "doi-url": "@id",
    "type": "@type",
    "issued": {"@type": "xsd:date"},
    "published-print": {"@type": "xsd:date"},
    "published-online": {"@type": "xsd:date"},
    "date-time": {"type": "xsd:dateTime"},
    "authors": {"@id": "doib:author"},
    "instances": {"@id": "doib:instance"}
  },
 "@graph": 
EOF
  print "[\n" if $opt_j; # JSON array start
}

END {
  print "]" if $opt_j;  # JSON array end
  print "}" if $opt_l;  # JSONLD end
}

print ",\n" if ($opt_j || $opt_l) && $. > 1;  # JSON array separator

s{^"(.*)"$}{$1};                              # strip surrounding quotes
s{'\\"delay-in-days'}{'delay-in-days'}g;      # this field name starts with double quote (wicked)
s{['\w-]+': (None|\[\]|u'UNKNOWN')}{}g;       # kill null/empty/useless values
s{, (?=,)}{}g;                                # remove doubled comma delimiters resulting from prev step
s{([\[\{]), }{$1}g;                           # remove leading comma delimiter
s{, ([\]\}])}{$1}g;                           # remove trailing comma delimiter
s{-(\d)-}{-0$1-}g;                            # fix dates to 2-digit month
s{-(\d)'}{-0$1'}g;                            # fix dates to 2-digit day
s{(?<='doi': u')([^']+)}{\L$1}g if $opt_d;    # normalize DOI by lowercasing it
s{\\\\'}{\\'}g;                               # double backslash escaping of apostrophe to single escaping
s{\bu'(.*?)(?<!\\)'}{                         # JSON strings don't have a u'..' prefix
  local $_ = $1;
  s{\\'}{'}g;                                 # apostrophes in JSON don't need (nor admit) backslash escapes
  qq{"$_"}                                    # delimit the string with double quotes
}ge;
s{\bu\\"(.*?)\\"}{"$1"}g;                     # same as above line
s{'UnpayWall'}{"UnpayWall"}g;                 # sometimes 'UnpayWall' appears without prefix u'..'
s{'([a-z-]+|collectedFrom)'}{"$1"}g;          # JSON keys should use double quotes not single quotes
s{\\x}{\\u00}g;                               # JSON uses unicode escapes \uXXXX rather than \xXX. Pray we get valid unicode chars
s{\\\\\\\\u}{\\u}g;                           # fix quadruple backslashes to single backslashes
s{\\\\u}{\\u}g;                               # fix double backslashes to single backslashes, eg in \\u2018juridique\\u2019
s{(?<=http://dx.doi.org/)(\S+)}{
  local $_ = $1;
  $_ = lc if $opt_d;                          # normalize DOI by lowercasing it
  s{([<>])}{sprintf("%%%2x",ord($1))}ge;      # URL-encode special chars
  $_
}e;
