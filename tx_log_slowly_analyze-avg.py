import json
import time
import datetime
import os, sys
from elasticsearch import Elasticsearch


############################
##### Parameter Region #####
############################

### Elasticsearch Parameter
indexname = 'txlog_avg_time_analysis'
es_url = 'http://10.51.85.251:9200/'


###  Look forward how many days?
day_tuning = 2


###  Do you want to generate how many files?
files = 1



####################################################################
##### Get the from_datetime & to_datetime in yyyy-MM-dd format #####
####################################################################

def get_from_to_datetime(day_tuning_var):
	from_datetime = datetime.date.today() - datetime.timedelta(day_tuning_var)
	return from_datetime

	
	
###################################################
##### Transfer date time to epoch time format #####
###################################################

def datetime_to_epochtime(dt):
	timeformat = '%Y-%m-%d'
	time.strptime(dt,timeformat)
	s=time.mktime(time.strptime(dt,timeformat))
	return int(s)

	
	
#######################################################	
##### Get the Unique Count of TX_ID of assign day #####
#######################################################	

def unique_count_of_txid(day_tuning_var):	
	es = Elasticsearch([es_url])
	unique_count = es.search(
		index = 'txlog-*',
		size = 0,
		body = '{\
			"query": {\
				"bool": {\
					"must" : [\
					{\
						"range" : {\
							"@timestamp" : {\
							  "gte": "now-' + str(day_tuning_var) + 'd/d",\
							  "lte": "now-' + str(day_tuning_var) + 'd/d",\
							  "time_zone": "+08:00",\
							  "format": "yyyy-MM-dd HH:mm:ss"\
							}\
						}\
					}\
					],\
					"must_not": [\
						{"term": {"Return_code.keyword": "Incoming"}},\
						{"term": {"Tx_message.keyword": "response"}}\
					]\
				}\
			},\
			"aggs": {\
				"Transaction_ID_count": {\
					"cardinality": {\
						"field": "Transaction_ID.keyword"\
					}\
				}\
			}\
		}'
	)
	# return json.dumps(unique_count['aggregations']['Transaction_ID_count']['value'])
	return unique_count['aggregations']['Transaction_ID_count']['value']

	
	
def get_txid_data(day_tuning_var, txid_size):	
	es = Elasticsearch([es_url])
	tx_dataset = es.search(
		index = 'txlog-*',
		size = 0,
		body = '{\
			"query": {\
				"bool": {\
					"must" : [\
					{\
						"range" : {\
							"@timestamp" : {\
							  "gte": "now-' + str(day_tuning_var) + 'd/d",\
							  "lte": "now-' + str(day_tuning_var) + 'd/d",\
							  "time_zone": "+08:00",\
							  "format": "yyyy-MM-dd HH:mm:ss"\
							}\
						}\
					}\
					],\
					"must_not": [\
						{"term": {"Return_code.keyword": "Incoming"}},\
						{"term": {"Tx_message.keyword": "response"}}\
					]\
				}\
			},\
			"aggs": {\
				"data_bucket": {\
					"terms": {\
						"field": "Transaction_ID.keyword",\
						"size": ' + str(txid_size) + ',\
						"order": {\
							"txid_average": "asc"\
						}\
					},\
					"aggs": {\
						"txid_average": {\
							"avg": {\
								"field": "Tx_time_diff"\
							}\
						}\
					}\
				}\
			}\
		}'
	)
	# return json.dumps(unique_count['aggregations']['Transaction_ID_count']['value'])
	return tx_dataset



def get_total_hits(day):
	total_hits_num = get_txid_data(day, unique_count_of_txid(day))['hits']['total']
	return total_hits_num


	
###########################################
##### Put the Data into Elasticsearch #####
###########################################

def index_data_to_elasticsearch(indexname_var, datetime_var, Transaction_ID_var, today_weight_var, yesterday_weight_var, weight_difference_var, data_status_flag_var):
	es = Elasticsearch([es_url])
	es_indexname = indexname_var + '-' + str(datetime_var.strftime('%Y'))
	type_name = indexname_var
	es_timestamp = str(datetime_var.strftime("%Y-%m-%d"))
	### yesterday has None, we need transform it to zero.
	if yesterday_weight_var is None:
		yesterday_weight_var = 0
	es.index(
		index = es_indexname,
		doc_type = type_name,
		body = '{\
			"Transaction_ID" : "' + Transaction_ID_var + '",\
			"today_avg" : ' + str(today_weight_var) + ',\
			"yesterday_avg" : ' + str(yesterday_weight_var) + ',\
			"avg_difference" : ' + str(weight_difference_var) + ',\
			"data_status_flag" : ' + str(data_status_flag_var) + ',\
			"@timestamp" : "' + str(es_timestamp) + '"\
		}'
	)

	
### element is elasticsearch json data block, element['key'] is the value in key field
"""     a data block of elasticsearch json like following
{
	"key": "TXNPCP09",
	"doc_count": 11,
	"extend_stat": {
	"count": 11,
	"min": 0.42899999022483826,
	"max": 1.5479999780654907,
	"avg": 0.8662727122957056,
	"sum": 9.528999835252762,
	"sum_of_squares": 9.35678066440249,
	"variance": 0.10018801196843151,
	"std_deviation": 0.3165248994446274,
	"std_deviation_bounds": {
	  "upper": 1.4993225111849604,
	  "lower": 0.23322291340645074
	}
	},
	"coefficient_of_variation": {
	"value": 0.3653871291937687
	}
},
"""	



####################################
##### Main Fuction Start Here. #####
####################################
	
for i in range(files):

	################################
	##### Prepare Today's Data #####
	################################
	
	### Get Today's Datetime
	today_datetime = get_from_to_datetime(day_tuning + i)
	
	print("Today's TXID Average List ===== " + str(today_datetime) + "=====" )
	
	### Declare today_dict is a Dictionary Structure
	today_dict = {}
	
	### To get all the txid avg value, We need the unique_count_of_txid as the parameter of the [aggs][data_bucket][terms][size] in 'get_txid_data' elasticsearch request 
	for element in get_txid_data(day_tuning + i, unique_count_of_txid(day_tuning + i))['aggregations']['data_bucket']['buckets']:
	
		### Explain: element['key'] as ("key": "TXNPCP09"); 
		print(element['key'], str(element['txid_average']['value']), sep=', ')
		today_dict[element['key']] = element['txid_average']['value']
	
	
	
	####################################
	##### Prepare Yesterday's Data #####
	####################################
	
	##### Get Yesterday's Datetime
	yesterday_datetime = get_from_to_datetime(day_tuning + 1 + i)
	
	print("Yesterday's TXID Average List ===== " + str(yesterday_datetime) + "=====" )

	##### Declareto yesterday_dict is a Dictionary Structure
	yesterday_dict = {}

	for element in get_txid_data(day_tuning + 1 + i, unique_count_of_txid(day_tuning + 1 + i))['aggregations']['data_bucket']['buckets']:
		print(element['key'], str(element['txid_average']['value']), sep=', ')
		yesterday_dict[element['key']] = element['txid_average']['value']
	
	
	
	
	###############################################################################################################################################################################
    ##### Make the Result. Format: ( 'tx_id', today_avg, yesterday_avg, (1|0) ). Explain: '1' as both today & yesterday have data; '0' as today have data but yesterday none. #####
	###############################################################################################################################################################################
	
	my_list = []
	
	for key, value in today_dict.items():
		if yesterday_dict.get(key, 'empty') != 'empty':
			my_list.append((key, today_dict.get(key), yesterday_dict.get(key), today_dict.get(key) - yesterday_dict.get(key), 1))
		else:
			my_list.append((key, today_dict.get(key), yesterday_dict.get(key), float(today_dict.get(key)), 0))
			
	sorted_my_list = sorted(my_list, key=lambda x: x[3], reverse=True)

	

	
	
	#############################
    ##### Store the Result. #####
	#############################
	
	# f = open( str(get_from_to_datetime(day_tuning+i)) + 'VS' + str(get_from_to_datetime(day_tuning+1+i)) + '.csv', 'w' )
	
	for elem in sorted_my_list:
		print (elem)
		
		### Here put the data to elasticsearch
		#index_data_to_elasticsearch(indexname, get_from_to_datetime(day_tuning+i), elem[0], elem[1], elem[2], elem[3], elem[4])
		
		'''
		f.write( str(elem[0]) + ',' + str(elem[1]) + ',' + str(elem[2]) + ',' + str(elem[3]) + ',' + str(elem[4]) )
		f.write('\n')
		'''
	
	print('==========================================================================================')

print('ttt')
print('ttt')
print('ttt')