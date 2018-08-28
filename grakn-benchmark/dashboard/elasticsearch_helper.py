import elasticsearch
import elasticsearch.helpers
import json

class ElasticsearchUtility(object):

    def __init__(self, index_name):
        self.es = elasticsearch.ElasticSearch()
        self.index = index_name

    
    def get_all_experiment_names(self):
        pass

    def get_spans_with_experiment_name(self, experiment_name):
        pass

    def get_spans_for_trace_id(self, trace_id):
        pass


