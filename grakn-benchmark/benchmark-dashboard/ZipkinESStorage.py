import elasticsearch
import elasticsearch.helpers as helpers
import json

class ZipkinESStorage(object):

    def __init__(self, indices="benchmarking:*"):
        """ Create a connection to ES _and_ check that the required templates are in it """
        self.es = elasticsearch.Elasticsearch()
        self.indices = indices

    def get_all_execution_names(self, field="tags.executionName", include_filter_regexp="", size=10000, doc_type='span'):
        body = {
            "size": 0,
            "aggs" : {
                "aggregated" : {
                    "terms" : {
                        "field" : field,
                        "size": size 
                    }
                }
            }
        }
        filter_regexp = include_filter_regexp.strip()
        if filter_regexp != "":
            body["aggs"]["aggregated"]["terms"]["include"] = filter_regexp

        aggregate = self.es.search(
                index=self.indices,
                doc_type=doc_type,
                body=body)

        if aggregate['hits']['total'] == 0:
            return set([])

        aggregated_names_buckets = aggregate['aggregations']['aggregated']['buckets']
        execution_names_set = set([x['key'] for x in aggregated_names_buckets])
        return execution_names_set


    def get_spans_with_execution_name(self, experiment_name, field="tags.executionName", doc_type='span'):
        query = {
            "query": {
                "term": {
                    field: experiment_name
                    }
                }
            }

        result_iter = helpers.scan(self.es,
                index=self.indices,
                doc_type=doc_type,
                query=query)

        return [doc['_source'] for doc in result_iter]


    def get_spans_with_parent(self, parent_id, doc_type='span', sorting=None):
        query = {
            "query": {
                "term": {
                    "parentId": parent_id 
                    }
                },
            "sort": [
            ]
        }

        if sorting is not None:
            query['sort'].append(sorting)

        spans_iter = helpers.scan(self.es,
                index = self.indices,
                doc_type=doc_type,
                preserve_order=True, # NOTE this may be expensive!
                query=query)

        return [doc['_source'] for doc in spans_iter]


    def get_number_of_children(self, parent_id, doc_type='span'):
        children = self.get_spans_with_parent(parent_id)
        return len(children)


    def get_named_span_with_parents(self, name, parent_ids, doc_type='span'):
        """ Retrieve spans with any of the given parent_ids """
        should_terms = [{"term": { "parentId": id}} for id in parent_ids]

        query = {
            "query": {
                "bool": {
                    "should": should_terms
                    }
                }
            }

        spans_iter = helpers.scan(self.es,
                index=self.indices,
                doc_type=doc_type,
                query=query)

        return [doc['_source'] for doc in spans_iter]


    def get_child_names(self, parent_id, doc_type='span', max_children=10000):
        """ Retrieve via aggregation the names of the child spans for grouping by later """
        query = {
            "query": {
                "term": {
                    "parentId": parent_id
                    }
                },
            "aggs": {
                "group_by_name" : {
                    "terms": {
                        "field": "name",
                        "size": max_children
                        }
                    }
                },
            "size": 0
            }

        child_names_aggregated = self.es.search(
                index=self.indices,
                doc_type=doc_type,
                size=10000,
                body=query)


        # TODO figure out why this is still returning hits that formed the aggregate even though size is 0
        # print(child_names_aggregated)

        return [bucket['key'] for bucket in child_names_aggregated['aggregations']['group_by_name']['buckets']]







