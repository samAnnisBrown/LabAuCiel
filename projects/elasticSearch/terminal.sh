#!/usr/bin/env bash

# Create Snapshot
curl -XPUT 'search-wildwest-elasticsearch-3odybwxjscnkxgvkdtoz4hdc2y.ap-southeast-2.es.amazonaws.com/_snapshot/es-snapshots/es55backup'
# List Snapshots in Repository
curl -XGET 'search-wildwest-elasticsearch-3odybwxjscnkxgvkdtoz4hdc2y.ap-southeast-2.es.amazonaws.com/_snapshot/es-snapshots/_all?pretty'

# ---
curl -XGET 'search-ww-es-6-m6req6yocsam2tnhjxw2hvqo5m.ap-southeast-2.es.amazonaws.com/_snapshot?pretty'
curl -XGET 'search-ww-es-6-m6req6yocsam2tnhjxw2hvqo5m.ap-southeast-2.es.amazonaws.com/_snapshot/es-snapshots/_all?pretty'
curl -XDELETE 'search-ww-es-6-m6req6yocsam2tnhjxw2hvqo5m.ap-southeast-2.es.amazonaws.com/_all'
# ---

# Restore Snapshot
curl -XPOST 'search-ww-es-6-m6req6yocsam2tnhjxw2hvqo5m.ap-southeast-2.es.amazonaws.com/_snapshot/es-snapshots/es55backup/_restore' -H 'Content-Type: application/json' -d '{"indices": [ "ddbstream-20171129", "cbr-quicksight_redshift_costreports-201712", "cbr-quicksight_redshift_costreports-201711", "alblogs1-20171127", "adhoc-test" ]}'

# List Indices
curl -XGET 'search-wwes-75dyceauwq2lk6pg3kf5w4254y.ap-southeast-2.es.amazonaws.com/_cat/indices?v&pretty'

# Delete Index
curl -XDELETE 'vpc-aws-cost-analysis-hmr7dskev6kmznsmqzhmv7r3te.ap-southeast-2.es.amazonaws.com/cbr-daily-cost-report-201711?pretty'

lineItem_Operation:RunInstances* AND lineItem_UsageType:*BoxUsage*

