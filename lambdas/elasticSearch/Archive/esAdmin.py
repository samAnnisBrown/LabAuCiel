import sys
import requests

esHost = "vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com"   # Elasticsearch Domain


def list_indices():
    response = requests.get('http://' + esHost + '/_cat/indices?v&pretty')
    return response.text


def del_index(indexName):
    url = 'http://' + esHost + '/' + indexName + '?pretty'
    response = requests.delete(url)
    return response.text


if '-l' in sys.argv:
    print(list_indices())

if sys.argv[1] == '-d':
    if sys.argv[2] is not None:
        print(del_index(sys.argv[2]))
    else:
        print('No index listed')

else:
    print('Nothing')