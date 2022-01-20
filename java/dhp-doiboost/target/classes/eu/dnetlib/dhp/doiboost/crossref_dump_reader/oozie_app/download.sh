#!/bin/bash
curl -LSs -H "Crossref-Plus-API-Token: Bearer $4"  $1 | hdfs dfs -put - $2/$3