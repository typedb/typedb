import dash_core_components as dcc
import dash_html_components as html

class ExecutionSelector(object):
    def __init__(self, sorted_executions):
        self._sorted_executions = sorted_executions

    def get_layout(self):
        """ Generate HTML for the list of benchmark executions as a column """
        executions_radio = dcc.RadioItems(
            id="execution-selector-radio",
            options=[{'label': x, 'value': x} for x in self._sorted_executions],
            value=self._sorted_executions[0]  # initialize with most recent one in sorted list
        )
        return html.Div(
            children=[
                html.H3("Executions"),
                executions_radio
            ],
            className="col-xl-1"
        )

    # TODO
    def onClick(self, *args):
        pass
