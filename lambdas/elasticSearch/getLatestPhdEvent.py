import json
import urllib.request


def getLatestPhdEvent():
    # Variables
    #es = 'https://vpc-sydney-summit-2018-ukfuj6urblh2nzu3revuq43dze.us-east-1.es.amazonaws.com"        # Works from Lambda in us-east-1 default VPC
    es = 'https://es.annisbrown.com'                                                                    # Works from ANT (IP Whitelisted)
    index = 'phd-events'
    query = {
        "query": {
            "query_string": {
                "default_field": "ElasticSearchUpload",
                "query": "Success"
            }
        },
        "size": 1,
        "sort": [
            {
                "PhdEventTime": {
                    "order": "desc"
                }
            }
        ]
    }
    # Elasticsearch Request/Response
    payload = json.dumps(query).encode('utf-8')         # Encode query for HTTP request
    request = urllib.request.Request(es + '/' + index + '/_search', payload, {'Content-Type': 'application/json'}, method='GET')    # Build HTTP request
    response = urllib.request.urlopen(request).read()   # Send Request
    response = json.loads(response.decode('utf-8'))     # Decode response and convert to JSON

    return response['hits']['hits'][0]['_source']       # Return query payload


print(getLatestPhdEvent())
