import dash
import dash_html_components as html
import dash_core_components as dcc
import datetime
import pandas as pd
import plotly.graph_objs as go

from collections import Counter
from SpanList import SpanList
from ZipkinESStorage import ZipkinESStorage

class Dashboard(object):

    def __init__(self, max_interactive_graphs_per_execution=100):

        self.zipkinESStorage = ZipkinESStorage()
        self._sorted_executions = self._get_sorted_executions()
        # cache of previously selected executions so they don't have to be recomputed
        # also used for predeclared callback lookups
        self._executions = {}

        self._app = dash.Dash()
        self._execution_selector = Dashboard.ExecutionSelector(self._sorted_executions)
        self._app.layout = self._make_layout()
        self._app.config.suppress_callback_exceptions = True
        self._app.css.append_css({
            'external_url': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css'
        })
        self._create_static_callbacks()
        self._create_dynamic_callbacks(max_interactive_graphs_per_execution)

    def run(self, debug=True):
        self._app.run_server(debug=debug)

    def _create_static_callbacks(self):
        # ---- static callbacks, always required ----
        @self._app.callback(
            dash.dependencies.Output('active-benchmark', 'children'),
            [dash.dependencies.Input('execution-selector-radio', 'value')])
        def execution_updated(execution_name):
            self._upsert_execution(execution_name)
            return self._executions[execution_name].get_layout()

    def _create_dynamic_callbacks(self, max_graphs):
        """ pre-compute the controls we will need to generate graphs, all callbacks must be declared before server starts """

        for i, execution_name in enumerate(self._sorted_executions):
            # create a app.callback for each possible required callback in BenchmarkExecutionComponent

            def route_execution_callback(method_name, exec_name):
                # NOTE pass exec_name through to retain a copy from the loop, else python closures refer to last loop iter
                print("creating callback router for {1}.{0}.method_name".format(method_name, exec_name))

                def wrapped_callback(*args):
                    # copy execution name using a lambda
                    print("Callback with args, method aname and execution name: {0}, {1}, {2}".format(args, method_name,
                                                                                                      exec_name))
                    self._upsert_execution(exec_name)
                    execution = self._executions[exec_name]
                    return execution.route_predeclared_callback(method_name, *args)

                return wrapped_callback

            callback_definitions = Dashboard.ExecutionVisualiser.get_predeclared_callbacks(execution_number=i,
                                                                                 graph_callbacks=max_graphs)

            for callback_function_name in callback_definitions:
                callback_definition = callback_definitions[callback_function_name]
                self._app.callback(callback_definition[0], callback_definition[1])(
                    route_execution_callback(callback_function_name, execution_name))

    def _upsert_execution(self, execution_name):
        """ Create an execution if it doesn't already exist in the previously loaded executions cache"""
        if execution_name not in self._executions:
            execution_number = self._sorted_executions.index(execution_name)
            self._executions[execution_name] = Dashboard.ExecutionVisualiser(self.zipkinESStorage,
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

    class ExecutionVisualiser(object):
        def __init__(self, zipkinESStorage, execution_name, execution_number):
            print("Creating BenchmarkExecutionComponent...")

            self._zipkinESStorage = zipkinESStorage
            self._execution_name = execution_name
            self._execution_number = execution_number

            # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint
            # note: this builds for all the queries right away since this isn't that expensive at this point
            self.overview_data = None
            self.repetitions = None
            self._build_overview_data()
            # overview_data's indexes are pre-sorted for effiency, use this throughout
            self.sorted_all_concept_numbers = self.overview_data.index.unique().tolist()
            self.sorted_all_queries = self.overview_data.columns.unique(0).tolist()
            # double check sorting wasn't forgotten
            assert Dashboard.ExecutionVisualiser.inorder(self.sorted_all_concept_numbers), "Concept numbers not sorted!"
            assert Dashboard.ExecutionVisualiser.inorder(self.sorted_all_queries), "Queries not sorted!"
            # pandas dataframe for storing the children of the toplevel queries, used for aggregation and drill down
            self.toplevel_query_breakdown = None
            self._allocate_toplevel_query_breakdown()

            # look up predefined graph IDs to local objects
            self.spanlists_graphs_id_map = {}

            print("...finished creating BenchmarkExecutionComponent")

        # TODO
        def onQuerySelectorClick(self, *args):
            pass

        @staticmethod
        def get_predeclared_callbacks(execution_number, graph_callbacks=100):
            """
            !IMPORTANT!
            Static method used to predefine a load of callbacks. This needs to be done since Dash can't
            dynamically create INPUT html elements -- all callbacks must be defined before the server starts!
            """

            # the callbacks for the query selector dropdown & number of concepts to break down
            rendering_callbacks = {
                '_render_overview': (
                    dash.dependencies.Output("overview-{0}".format(execution_number), "children"),
                    [
                        dash.dependencies.Input("query-selector-{0}".format(execution_number), "value")
                    ]
                ),
                '_render_query_breakdown': (
                    dash.dependencies.Output("query-breakdowns-{0}".format(execution_number), "children"),
                    [
                        dash.dependencies.Input("concepts-radio-{0}".format(execution_number), "value"),
                        dash.dependencies.Input("query-selector-{0}".format(execution_number), "value")
                    ]
                )
            }

            print("***** Only predefining callbacks for 100 graphs for any benchmark execution! *****")
            print("***** Increase this number if needed via --max-graphs (unknown performance impact) *****")

            # predefine callbacks to reference clicks in the graphs
            # NOTE: this requires we uses these same IDs when actually instantiating
            # any graphs in BenchmarkExecutionComponents
            for i in range(graph_callbacks):
                # `i` corresponds the query number/set of graphs generated
                # MUST format graph IDS with this!
                graph_one_id = 'breakdown-graph-{0}-{1}-{2}'.format(execution_number, i, 0)
                graph_two_id = 'breakdown-graph-{0}-{1}-{2}'.format(execution_number, i, 1)
                rendering_callbacks[graph_one_id] = (
                    dash.dependencies.Output(graph_one_id, 'figure'),
                    [dash.dependencies.Input(graph_one_id, 'clickData')]
                )
                rendering_callbacks[graph_two_id] = (
                    dash.dependencies.Output(graph_two_id, 'figure'),
                    [dash.dependencies.Input(graph_two_id, 'clickData')]
                )

            return rendering_callbacks

        def route_predeclared_callback(self, method_name, *args):
            print("Routing callback for: {0}...".format(method_name))
            if method_name == '_render_overview':
                return self._render_overview(*args)
            elif method_name == '_render_query_breakdown':
                return self._render_query_breakdown(*args)
            elif method_name.startswith('breakdown-graph'):
                graph_id = method_name

                # need to check if already instantiated, callbacks can be out of order?
                # also this callback is sometimes triggered with (None, ) as args

                if args[0] is None:
                    # so we get a clickData callback with (None, ) when the graph is created
                    # future callbacks only work if we raise an Exception here??
                    # Probably because we can't return the actual figure yet, and returning {} or {'data':{}, 'layout':{}}
                    # means the graph is empty in the DOM even if displayed properly?
                    raise Exception("lol")
                if args[0] is not None and graph_id in self.spanlists_graphs_id_map:
                    spanlists_graph = self.spanlists_graphs_id_map[graph_id]

                    clicked_points = args[0]['points']

                    # one click could hit multiple levels or curves, find majority
                    levels_clicked = [pt['y'] for pt in clicked_points]
                    level_counts = Counter(levels_clicked)
                    most_common_level = level_counts.most_common(1)[0][0]

                    curves_clicked = [pt['curveNumber'] for pt in clicked_points]
                    curve_counts = Counter(curves_clicked)
                    most_common_curve_number = curve_counts.most_common(1)[0][0]
                    # convert overall curve number into an offset AT a level
                    level_span_number = spanlists_graph.curve_number_to_span_number_at_level(most_common_curve_number)

                    # update the spanlists graph!
                    print("Expanding graph at: {0}, {1}".format(most_common_level, level_span_number))
                    spanlists_graph.expand_span_at_level_name(most_common_level, level_span_number)
                    return spanlists_graph.get_figure()
                else:
                    return {}
            else:
                print("Unknown method name in route_callback: {0}".format(method_name))

        def get_layout(self):
            print("BenchmarkExecutionComponent.full_render...")

            query_selector = dcc.Dropdown(
                id='query-selector-{0}'.format(self._execution_number),
                options=[{'label':q, 'value':q} for q in self.sorted_all_queries],
                value=self.sorted_all_queries,
                multi=True
            )
            concepts_selector = dcc.RadioItems(
                id='concepts-radio-{0}'.format(self._execution_name),
                options=[{'label': n, 'value': n} for n in self.sorted_all_concept_numbers],
                value=self.sorted_all_concept_numbers[-1]
            )

            rendered = html.Div(
                id='component-{0}'.format(self._execution_number),
                children=[
                    html.H5("Repetitions: {0}".format(self.repetitions)),
                    html.Div(
                        children=[
                            html.H5("Queries executed"),
                            query_selector
                        ]
                    ),
                    html.Div(
                        id='overview-{0}'.format(self._execution_number)
                    ),
                    html.Div(children=[
                        html.H5("Number of concepts benchmarked"),
                        concepts_selector
                        ]
                    ),
                    html.Div(
                        id='query-breakdowns-{0}'.format(self._execution_number)
                    )
                ]
            )

            return rendered

        @staticmethod
        def inorder(l, reverse=False):
            """
            :param l:List -
            :param reverse:bool - sort check descending if True, else ascending
            :return: bool - return if the list is sorted in ascending order (descending if reverse=True)
            """
            if not reverse:
                return all(a <= b for (a, b) in zip(l, l[1:]))
            else:
                return all(b <= a for (a, b) in zip(l, l[1:]))


if __name__ == '__main__':
    dashboard = Dashboard()
    dashboard.run()

