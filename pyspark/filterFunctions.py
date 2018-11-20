def empty_str(x):
	return x is None or len(x) ==0


def filter_record(x):
	if x['title'] is None or len(x['title']) == 0:
		x['record-quality-report'] = 'incomplete'
		return json.dumps(x, ensure_ascii=False)   

	for t in x['title']:
		if len(t.lower().replace('test', '').strip()) <3:
			x['record-quality-report'] = 'mock'
			return json.dumps(x, ensure_ascii=False)   

	if x['authors'] is None or len(x['authors']) == 0:
		x['record-quality-report'] = 'incomplete'
		return json.dumps(x, ensure_ascii=False)   


	if empty_str(x['accepted']) and empty_str(x['issued']) and empty_str(x['published-print']) and empty_str(x['published-online']):
		x['record-quality-report'] = 'incomplete'
		return json.dumps(x, ensure_ascii=False)   

	x['record-quality-report'] = 'complete'
	return json.dumps(x, ensure_ascii=False)   



def merge_items(x):
	m_item = None
	d_item = None

	for item in x[1]:
		if type(item) == dict:
			d_item = item
		else:
			m_item = item

	if m_item is not None:
		if d_item['instances'] is None:
			d_item['instances'] = []
		d_item['instances'].append({'access-rights': 'UNKNOWN', 'provenance': 'MAG', 'url': 'https://academic.microsoft.com/#/detail/'+m_item})
	return json.dumps(d_item, , ensure_ascii=False)   



doi_boost = sc.textFile('/data/df/doiBoost_openaire').map(json.loads).map(lambda x: (x['doi'],x))
mic = sc.textFile('/data/MicrosoftDump/MicrosoftPapers').map(eval).filter(lambda x: x['DOI'] is not None and len(x['DOI']) >0).map(lambda x: (x['DOI'], x['PaperId']))


f = doi_boost + mic
f.groupByKey().map(merge_items).first()