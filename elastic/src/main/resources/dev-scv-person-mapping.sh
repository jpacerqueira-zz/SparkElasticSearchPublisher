#!/bin/bash
#date format: dt=YYYYMMDD

dt=$1
index_prefix="dev-gfansview"
es_type="person"
es_node="localhost" #"localhost"

echo "create base http://"${es_node}":9200/${index_prefix}/ WARNING! Daily Indexes Deleted"
echo "try curl -XDETELE http://"${es_node}":9200/${index_prefix}/"
curl -XDELETE 'http://'${es_node}':9200/'${index_prefix}'/' || true 
echo "try curl -XPUT http://'${es_node}':9200/'${index_prefix}'/'"
curl -XPUT 'http://'${es_node}':9200'/${index_prefix}'/' || true
#date format: dt=YYYYMMDD
curl -XDELETE 'http://'${es_node}':9200/'${index_prefix}'-'${dt}'/'${es_type} || true
curl -XPUT 'http://'${es_node}':9200/'${index_prefix}'-'${dt} || true
#curl -XPUT 'http://'${es_node}':9200/'${index_prefix}'-'${dt}'/_mapping/'${es_type} -d @${es_type}'-mapping.json' || true
curl -XPUT 'http://'${es_node}':9200/'${index_prefix}'-'${dt}'/_mapping/'${es_type} -d '
{
    "person": {
        "properties": {
            "UID_GIGYA":{"type":"string", "null_value": "null"},
            "CREATED_DATE":{"type":"string", "null_value": "null"},
            "LAST_LOGIN_TIMESTAMP":{"type":"integer", "null_value": -1},
            "LAST_LOGIN_DATE":{"type":"date", "format": "yyyy-MM-dd HH:mm:ss || dateOptionalTime"},
            "SOCIAL_PROVIDER":{"type":"string", "null_value": "null"}
        }
    }
}
' || true

