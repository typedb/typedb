import dash_core_components as dcc

class ConceptCountSelector(object):
    def __init__(self, execution_number, sorted_concept_counts):
        self._execution_number = execution_number
        self._sorted_concept_counts = sorted_concept_counts

    def get_layout(self):
        concept_count_selector = dcc.RadioItems(
            id='concepts-radio-{0}'.format(self._execution_number),
            options=[{'label': n, 'value': n} for n in self._sorted_concept_counts],
            value=self._sorted_concept_counts[-1]
        )
        return concept_count_selector
