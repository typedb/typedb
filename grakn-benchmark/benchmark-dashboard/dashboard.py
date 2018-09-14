import dash
import dash_html_components as html
import dash_core_components as dcc
import datetime

from ZipkinESStorage import ZipkinESStorage

class Dashboard(object):

    def __init__(self):
        self._app = dash.Dash()
        self.zipkinESStorage = ZipkinESStorage()
        # cache of previously selected executions so they don't have to be recomputed
        # also used for predeclared callback lookups
        self._executions = {}

        self._sorted_executions = self._get_sorted_executions()
        self._app.layout = self._make_layout(self._sorted_executions)
        self._app.css.append_css({
            'external_url': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css'
        })


    def run(self, debug=True):
        self._app.run_server(debug=debug)


    def _create_static_callbacks(self):
        # ---- static callbacks, always required ----
        @self.app.callback(
            dash.dependencies.Output('active-benchmark', 'children'),
            [dash.dependencies.Input('execution-selector-radio', 'value')])
        def execution_updated(execution_name):
            self._upsert_execution(execution_name)
            return self._executions[execution_name].full_render()

    def _upsert_execution(self, execution_name):
        """ Create an execution if it doesn't already exist in the previously loaded executions cache"""
        if execution_name not in self._executions:
            execution_number = self._sorted_executions.index(execution_name)
            self._executions[execution_name] = Dashboard.ExecutionVisualiser(self.zipkinESStorage,
                                                                             execution_name,
                                                                             execution_number)

    def _make_layout(self, sorted_executions, benchmark_width=11):
        print("Generating layout...")

        # placeholder for benchmark graphs with a specified width
        active_benchmark = html.Div(
            className="col-xl-{0}".format(benchmark_width),
            children=[
                html.Div(id='active-benchmark')
            ]
        )

        # sidebar with set width and radio buttons with executions
        sidebar = self._get_execution_selector(sorted_executions)

        layout = html.Div(children=[
            html.H1("Grakn Benchmarking Dashboard"),
            html.Div(
                className="container-fluid",
                children=[
                    html.Div(
                        className="row",
                        children=[
                            sidebar,
                            active_benchmark
                        ]
                    ),
                    html.Div(
                        id="test"
                    )

                ]
            )
        ])

        return layout

    def _get_execution_selector(self, sorted_executions):
        """ Generate HTML for the list of benchmark executions as a column """
        executions_radio = dcc.RadioItems(
            id="execution-selector-radio",
            options=[{'label': x, 'value': x} for x in sorted_executions],
            value=sorted_executions[0]  # initialize with most recent one in sorted list
        )
        return html.Div(
            children=[
                html.H3("Executions"),
                executions_radio
            ],
            className="col-xl-1"
        )

    def _get_sorted_executions(self):
        """ Obtain from elasticsearch the existing benchmarking executions and return sorted by date """
        existing_executions = self.zipkinESStorage.get_all_execution_names()
        # split and sort by formatted date
        date_format = "%Y-%m-%d %H:%M"
        parser = lambda date_string: datetime.datetime.strptime(date_string, date_format)
        pairs = [(parser(x[:x.find(':') + 2]), x) for x in existing_executions]
        pairs.sort(reverse=True, key=lambda pair: pair[0])
        return [x[1] for x in pairs]


    class ExecutionSelector(object):
        def __init__(self):
            pass

        def onClick(self, *args):
            pass

    class ExecutionVisualiser(object):
        def __init__(self):
            pass

        def onQuerySelectorClick(self, *args):
            pass




if __name__ == '__main__':
    dashboard = Dashboard()
    dashboard.run()

