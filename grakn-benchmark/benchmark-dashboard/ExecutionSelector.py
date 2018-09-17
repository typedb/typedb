import dash_core_components as dcc
import dash_html_components as html

class ExecutionSelector(object):
    def __init__(self, executions):
        self._executions = executions

    def get_layout(self):
        """ Generate HTML for the list of benchmark executions as a column """

        if len(self._executions) > 0:
            executions_radio = dcc.RadioItems(
                id="execution-selector-radio",
                options=[{'label': x, 'value': x} for x in self._executions],
                value=self._executions[0]
            )
        else:
            executions_radio = dcc.RadioItems(
                id="execution-selector-radio",
                options=[{'label': x, 'value': x} for x in self._executions]
            )

        return html.Div(
            children=[
                html.H3("Executions"),
                executions_radio
            ],
            className="col-xl-1"
        )

