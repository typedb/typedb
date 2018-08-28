import elasticsearch
import elasticsearch.helpers
import json

es = elasticsearch.Elasticsearch()

# index='full_indexed_fixed_12'
index = 'max_1000_concepts_4'

# look up all the tags associated with records that have a 'query' string

tag_holders_iter = elasticsearch.helpers.scan(es, 
                    index=index,
                    doc_type="span",
                    query={
                        "query" : {
                            "exists": {"field": "tags.query"}
                            }
                        }
                    )

# tag_holders = es.search(index=index, _source_include=['tags', 'traceId'], body='{"query": {"exists": {"field": "tags.query"}}}')
# convert this into a lookup dict
traceId_tags_mapping = {}
import_counts = {}

for record in tag_holders_iter:
    trace_id = record['_source']['traceId']
    tags = record['_source']['tags']
    traceId_tags_mapping[trace_id] = tags
    import_counts[trace_id] = 0

print("Mapping:")
print(json.dumps(traceId_tags_mapping, indent=2))


# iterate over all the data NOT containing tags.query
# do this in pagination
sub_spans = elasticsearch.helpers.scan(es,
                index=index,
                doc_type="span",
                query={
                    "query": {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "tags.query"
                                    }
                                },
                            "must": {
                                "exists": {
                                   "field": "parentId"
                                   }
                                }
                            }
                        }
                    }
                )
counts_by_query = {}

for record in sub_spans:
    record_id = record['_id']
    parent_trace_id = record['_source']['parentId']
    if parent_trace_id not in traceId_tags_mapping:
        print("trace id missing: {0}".format(parent_trace_id))
        continue
    corresponding_tags = traceId_tags_mapping[parent_trace_id]
    query = corresponding_tags['query']
    if query not in counts_by_query:
        counts_by_query[query] = 0
    counts_by_query[query] += 1
    # perform an update on this ID
    es.update(index=index, doc_type='span', id=record_id, body={"doc": { "tags": corresponding_tags}})
    import_counts[parent_trace_id] += 1

print("Transferred data for: ")
print(json.dumps(import_counts, indent=2))

print("Counts by query: ")
print(counts_by_query)



    

