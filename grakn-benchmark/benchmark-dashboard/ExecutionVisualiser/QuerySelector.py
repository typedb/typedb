import dash_core_components as dcc

class QuerySelector(object):
    def __init__(self, execution_number, sorted_queries):
        self._execution_number = execution_number
        self._sorted_queries = sorted_queries

    def get_layout(self):
        query_selector = dcc.Dropdown(
            id='query-selector-{0}'.format(self._execution_number),
            options=[{'label':q, 'value':q} for q in self._sorted_queries],
            value=self._sorted_queries,
            multi=True
        )
        return query_selector
