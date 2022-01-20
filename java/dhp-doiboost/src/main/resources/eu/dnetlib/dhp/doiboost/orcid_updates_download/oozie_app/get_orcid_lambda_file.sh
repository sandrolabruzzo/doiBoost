wget -O /tmp/last_modified.csv.tar http://74804fb637bd8e2fba5b-e0a029c2f87486cddec3b416996a6057.r3.cf1.rackcdn.com/last_modified.csv.tar
hdfs dfs -copyFromLocal /tmp/last_modified.csv.tar /data/orcid_activities_2020/last_modified.csv.tar
rm -f /tmp/last_modified.csv.tar