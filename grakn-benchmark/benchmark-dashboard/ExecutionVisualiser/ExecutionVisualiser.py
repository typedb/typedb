import dash
import dash_html_components as html
import pandas as pd
from collections import Counter

from ExecutionVisualiser.ConceptCountSelector import ConceptCountSelector
from ExecutionVisualiser.QuerySelector import QuerySelector
from ExecutionVisualiser.OverviewGraph import OverviewGraph
from ExecutionVisualiser.RootSpansData import RootSpansData
from ExecutionVisualiser.BreakdownGraph import BreakdownGraph


class ExecutionVisualiser(object):
    """ The component that visualises all the data for a single execution of the benchmarking system """

    def __init__(self, zipkin_ES_storage, execution_name, execution_number):
        self._zipkin_ES_storage = zipkin_ES_storage
        self._execution_name = execution_name
        self._execution_number = execution_number
        self._repetitions = None    # will be assigned lazily

        # set up pandas dataframes
        self._init_data()
        # create visual components
        self._query_selector = QuerySelector(self._execution_number, self._sorted_queries)
        self._concept_count_selector = ConceptCountSelector(self._execution_number,
                                                                                self._sorted_concept_counts)
        self._overview_graph = OverviewGraph(self._execution_number, self._overview_dataframe)

        # initially empty container for storing the toplevel queries' data, used for aggregation and drill down
        self._root_spans_data = RootSpansData(
            zipkin_ES_storage=self._zipkin_ES_storage,
            overview_data_ref=self._overview_dataframe,
            sorted_queries=self._sorted_queries,
            sorted_concept_counts=self._sorted_concept_counts,
            repetitions=self._repetitions
        )

        # predefined graph html IDs to breakdown graphs
        self._html_id_to_breakdown_graph = {}


    def _init_data(self):
        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint
        # note: this builds for all the queries right away since this isn't that expensive at this point
        self._overview_dataframe = None
        self._build_overview_dataframe()

        # overview_data's indexes are pre-sorted for effiency, use this throughout
        self._sorted_concept_counts = self._overview_dataframe.index.unique().tolist()
        self._sorted_queries = self._overview_dataframe.columns.unique(0).tolist()
        # double check sorting wasn't forgotten
        assert ExecutionVisualiser.inorder(self._sorted_concept_counts), "Concept numbers not sorted!"
        assert ExecutionVisualiser.inorder(self._sorted_queries), "Queries not sorted!"


    @staticmethod
    def get_predeclared_callbacks(execution_number, max_interactive_graphs_per_execution=100):
        """
        !IMPORTANT!
        Static method used to predefine a load of callbacks. This needs to be done since Dash can't
        dynamically create INPUT html elements -- all callbacks must be defined before the server starts!
        """

        # the callbacks for the query selector dropdown & number of concepts to break down
        rendering_callbacks = {
            'get_overview_graph': (
                dash.dependencies.Output("overview-{0}".format(execution_number), "children"),
                [
                    dash.dependencies.Input("query-selector-{0}".format(execution_number), "value")
                ]
            ),
            'get_breakdown_graphs': (
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
        for i in range(max_interactive_graphs_per_execution):
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
        if method_name == 'get_overview_graph':
            return self.get_overview_graph(*args)
        elif method_name == 'get_breakdown_graphs':
            return self.get_breakdown_graphs(*args)
        elif method_name.startswith('breakdown-graph'):
            return self._route_graph_click_callback(method_name, *args)
        else:
            print("Unknown method name in route_callback: {0}".format(method_name))

    def _route_graph_click_callback(self, method_name, *args):
        graph_id = method_name

        # need to check if already instantiated, callbacks can be out of order?
        # also this callback is sometimes triggered with (None, ) as args

        if args[0] is None:
            # so we get a clickData callback with (None, ) when the graph is created
            # future callbacks only work if we raise an Exception here??
            # Probably because we can't return the actual figure yet, and returning {} or {'data':{}, 'layout':{}}
            # means the graph is empty in the DOM even if displayed properly?
            raise Exception("lol")
        if args[0] is not None and graph_id in self._html_id_to_breakdown_graph:
            breakdown_graph = self._html_id_to_breakdown_graph[graph_id]

            clicked_points = args[0]['points']
            curves_clicked = [pt['curveNumber'] for pt in clicked_points]
            curve_counts = Counter(curves_clicked)
            most_common_curve_number = curve_counts.most_common(1)[0][0]

            breakdown_graph.on_click(most_common_curve_number)
            return breakdown_graph.get_figure()
        else:
            return {}



    def get_layout(self):
        rendered = html.Div(
            id='component-{0}'.format(self._execution_number),
            children=[
                html.H5("Repetitions: {0}".format(self._repetitions)),
                html.Div(
                    children=[
                        html.H5("Queries executed"),
                        self._query_selector.get_layout()
                    ]
                ),
                html.Div(
                    id='overview-{0}'.format(self._execution_number)
                ),
                html.Div(children=[
                    html.H5("Number of concepts benchmarked"),
                    self._concept_count_selector.get_layout()
                ]
                ),
                html.Div(
                    id='query-breakdowns-{0}'.format(self._execution_number)
                )
            ]
        )

        return rendered

    # --- data functions ---

    def _build_overview_dataframe(self):
        print("Building overview data...")
        spans_for_execution = self._zipkin_ES_storage.get_spans_with_execution_name(self._execution_name)
        query_concepts_map = {} # collect data into a map
        for span in spans_for_execution:
            query = span['tags']['query']
            if self._repetitions is None:
                self._repetitions = int(span['tags']['repetitions'])
            batch_span_id = span['id']
            num_concepts = int(span['tags']['concepts'])
            duration_us = float(span['duration'])
            if (query, 'duration') not in query_concepts_map:
                query_concepts_map[(query, 'duration')] = {}
                query_concepts_map[(query, 'batchSpanId')] = {}
            query_concepts_map[(query, 'duration')][num_concepts] = duration_us
            query_concepts_map[(query, 'batchSpanId')][num_concepts] = batch_span_id

        # create a multi index that lets us access
        # in rows the number of concepts
        # and in columns query, duration or traceID columns (last two alternate)
        index, _ = pd.MultiIndex.from_tuples(query_concepts_map.keys(), names=['query', 'duration_traceid']).sortlevel()
        self._overview_dataframe = pd.DataFrame(query_concepts_map, columns=index)
        print("...finished building overview data")

    # --- end data functions ---

    def get_overview_graph(self, query_selector_value):
        filtered_overview_dataframe = self._overview_dataframe[query_selector_value]
        self._overview_graph = OverviewGraph(self._execution_number, filtered_overview_dataframe)
        return self._overview_graph.get_layout()


    def get_breakdown_graphs(self, concept_count, queries):
        """ Render the query breakdowns """

        # now have concepts_to_plot: int, and queries_to_plot: str[]

        # update root spans data with any missing columns
        self._root_spans_data.upsert_breakdown_for_concepts_and_queries(concept_count, queries)

        div = html.Div(
            children=[
                html.H3("Query Breakdowns -- {0} concepts".format(concept_count))
            ])

        # TODO reuse BreakdownGraph, especially if same data

        for query in queries:

            query_breakdown_container = html.Div(
                children=[
                    html.Hr(),
                    html.H5(query)
                ]
            )

            query_number = self._sorted_queries.index(query)

            # partition the root spans table into sub tables
            partition_names, partitioned_root_spans_data_collection_list = \
                self._root_spans_data.partition_for_query_and_concept_count(query, concept_count, partition_indices=[1])

            for (i, root_spans_data_collection) in enumerate(partitioned_root_spans_data_collection_list):
                graph_id = 'breakdown-graph-{0}-{1}-{2}'.format(self._execution_number, query_number, i)
                breakdown_graph = BreakdownGraph(
                    graph_name=partition_names[i],
                    graph_id=graph_id,
                    partitioned_root_spans_data_collection=root_spans_data_collection
                )
                self._html_id_to_breakdown_graph[graph_id] = breakdown_graph
                query_breakdown_container.children.append(breakdown_graph.get_layout())

            div.children.append(query_breakdown_container)

        return div


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
