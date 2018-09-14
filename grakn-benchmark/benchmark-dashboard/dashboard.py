import dash
import dash_html_components as html
import datetime

from ZipkinESStorage import ZipkinESStorage
from ExecutionSelector import ExecutionSelector
from ExecutionVisualiser import ExecutionVisualiser


class Dashboard(object):
    def __init__(self, max_interactive_graphs_per_execution=100):
        self._zipkinESStorage = ZipkinESStorage()
        self._execution_names = self._get_sorted_execution_names_from_zipkin()
        # cache of previously selected executions so they don't have to be recomputed
        # also used for predeclared callback lookups
        self._executions = {}

        self._execution_selector = ExecutionSelector(self._execution_names)
        self._dash = dash.Dash()
        self._dash.config.suppress_callback_exceptions = True
        self._create_static_callbacks()
        self._create_dynamic_callbacks(max_interactive_graphs_per_execution)
        self._dash.layout = self._make_layout()
        self._dash.css.append_css({
            'external_url': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css'
        })
        print("hi")

    def run(self, debug=True):
        self._dash.run_server(debug=debug)

    def _create_static_callbacks(self):
        # ---- static callbacks, always required ----
        @self._dash.callback(
            dash.dependencies.Output('active-benchmark', 'children'),
            [dash.dependencies.Input('execution-selector-radio', 'value')])
        def execution_updated(execution_name):
            self._upsert_execution(execution_name)
            return self._executions[execution_name].get_layout()

    def _create_dynamic_callbacks(self, max_interactive_graphs_per_execution):
        """ pre-compute the controls we will need to generate graphs, all callbacks must be declared before server starts """

        for execution_number, execution_name in enumerate(self._execution_names):
            # create a app.callback for each possible required callback in BenchmarkExecutionComponent

            def route_execution_callback(method_name, exec_name):
                # NOTE pass exec_name through to retain a copy from the loop, else python closures refer to last loop iter
                print("creating callback router for {1}.{0}".format(method_name, exec_name))
                def wrapped_callback(*args):
                    # copy execution name using a lambda
                    print("Callback with args, method aname and execution name: {0}, {1}, {2}".format(args, method_name,
                                                                                                      exec_name))
                    self._upsert_execution(exec_name)
                    execution = self._executions[exec_name]
                    return execution.route_predeclared_callback(method_name, *args)

                return wrapped_callback

            callback_definitions = ExecutionVisualiser.get_predeclared_callbacks(execution_number=execution_number,
                                                                                 max_interactive_graphs_per_execution=max_interactive_graphs_per_execution)
            for callback_function_name in callback_definitions:
                callback_definition = callback_definitions[callback_function_name]
                self._dash.callback(callback_definition[0], callback_definition[1])(route_execution_callback(callback_function_name, execution_name))

    def _upsert_execution(self, execution_name):
        """ Create an execution if it doesn't already exist in the previously loaded executions cache"""
        if execution_name not in self._executions:
            execution_number = self._execution_names.index(execution_name)
            self._executions[execution_name] = ExecutionVisualiser(self._zipkinESStorage,
                                                                   execution_name,
                                                                   execution_number)

    def _make_layout(self, benchmark_width=11):
        print("Generating layout...")

        # placeholder for benchmark graphs with a specified width
        active_benchmark = html.Div(
            className="col-xl-{0}".format(benchmark_width),
            children=[
                html.Div(id='active-benchmark')
            ]
        )

        layout = html.Div(children=[
            html.H1("Grakn Benchmarking Dashboard"),
            html.Div(
                className="container-fluid",
                children=[
                    html.Div(
                        className="row",
                        children=[
                            self._execution_selector.get_layout(),
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

    def _get_sorted_execution_names_from_zipkin(self):
        """ Obtain from elasticsearch the existing benchmarking executions and return sorted by date """
        existing_executions = self._zipkinESStorage.get_all_execution_names()
        # split and sort by formatted date
        date_format = "%Y-%m-%d %H:%M"
        parser = lambda date_string: datetime.datetime.strptime(date_string, date_format)
        pairs = [(parser(x[:x.find(':') + 2]), x) for x in existing_executions]
        pairs.sort(reverse=True, key=lambda pair: pair[0])
        return [x[1] for x in pairs]


if __name__ == '__main__':
    dashboard = Dashboard()
    dashboard.run()

