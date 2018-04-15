import argparse     # For all the aguments
import requests
import sys


parser = argparse.ArgumentParser(description="This scrupts uploads CUR data to and manipulate Elasticsearch indices")
# Working with Indexes
parser.add_argument('-l', '--index_list', action='store_true',
                    help='Lists the indices in the cluster described by the --elasticsearch_endpoint parameter.')
parser.add_argument('-d', '--index_delete',
                    help='Deletes an Elasticsearch index.  Enter the index name to delete')


args = parser.parse_args()


# List Indices
def listIndex(esEndpoint):
    response = requests.get('https://' + esEndpoint + '/_cat/indices?v&pretty')
    print(response.text)
    print('[LISTING] - Indices')
    sys.exit()


# Delete the specified ES index
def deleteElasticsearchIndex(indexName):
    print('[--DELETING--] - Index ' + indexName)
    es = returnElasticsearchAuth()
    es.indices.delete(index=indexName, ignore=[400, 404])


# Return ES auth, depending on whether it's in a Lambda function or not
def returnElasticsearchAuth():
    es = Elasticsearch(host=args.elasticsearch_endpoint,
                       port=80,
                       connection_class=RequestsHttpConnection)

    return es