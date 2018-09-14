import dash
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
import plotly.graph_objs as go

from collections import Counter
from SpanListsListGraph import SpanListsListGraph
from SpanList import SpanList

class ExecutionVisualiser(object):
    def __init__(self, zipkinESStorage, execution_name, execution_number):
        print("Creating BenchmarkExecutionComponent...")

        self._zipkinESStorage = zipkinESStorage
        self._execution_name = execution_name
        self._execution_number = execution_number

        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint
        # note: this builds for all the queries right away since this isn't that expensive at this point
        self._overview_data = None
        self._repetitions = None
        self._build_overview_data()
        # overview_data's indexes are pre-sorted for effiency, use this throughout
        self._sorted_all_concept_numbers = self._overview_data.index.unique().tolist()
        self._sorted_all_queries = self._overview_data.columns.unique(0).tolist()
        # double check sorting wasn't forgotten
        assert ExecutionVisualiser.inorder(self._sorted_all_concept_numbers), "Concept numbers not sorted!"
        assert ExecutionVisualiser.inorder(self._sorted_all_queries), "Queries not sorted!"
        # pandas dataframe for storing the children of the toplevel queries, used for aggregation and drill down
        self._toplevel_query_breakdown = None
        self._allocate_toplevel_query_breakdown()

        # look up predefined graph IDs to local objects
        self._spanlists_graphs_id_map = {}

        print("...finished creating BenchmarkExecutionComponent")

    # TODO
    def onQuerySelectorClick(self, *args):
        pass

    @staticmethod
    def get_predeclared_callbacks(execution_number, max_interactive_graphs_per_execution=100):
        """
        !IMPORTANT!
        Static method used to predefine a load of callbacks. This needs to be done since Dash can't
        dynamically create INPUT html elements -- all callbacks must be defined before the server starts!
        """

        # the callbacks for the query selector dropdown & number of concepts to break down
        rendering_callbacks = {
            '_get_overview_graph': (
                dash.dependencies.Output("overview-{0}".format(execution_number), "children"),
                [
                    dash.dependencies.Input("query-selector-{0}".format(execution_number), "value")
                ]
            ),
            '_get_query_breakdown_graphs': (
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
        if method_name == '_get_overview_graph':
            return self._get_overview_graph(*args)
        elif method_name == '_get_query_breakdown_graphs':
            return self._get_query_breakdown_graphs(*args)
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
            if args[0] is not None and graph_id in self._spanlists_graphs_id_map:
                spanlists_graph = self._spanlists_graphs_id_map[graph_id]

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
        print("BenchmarkExecutionComponent.get_layout...")

        query_selector = dcc.Dropdown(
            id='query-selector-{0}'.format(self._execution_number),
            options=[{'label':q, 'value':q} for q in self._sorted_all_queries],
            value=self._sorted_all_queries,
            multi=True
        )
        concepts_selector = dcc.RadioItems(
            id='concepts-radio-{0}'.format(self._execution_number),
            options=[{'label': n, 'value': n} for n in self._sorted_all_concept_numbers],
            value=self._sorted_all_concept_numbers[-1]
        )

        rendered = html.Div(
            id='component-{0}'.format(self._execution_number),
            children=[
                html.H5("Repetitions: {0}".format(self._repetitions)),
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

    # --- data functions ---

    def _build_overview_data(self):
        print("Building overview data...")
        spans_for_execution = self._zipkinESStorage.get_spans_with_execution_name(self._execution_name)
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
        self._overview_data = pd.DataFrame(query_concepts_map, columns=index)
        print("...finished building overview data")


    def _allocate_toplevel_query_breakdown(self):
        print("Begin allocating toplevel_query_breakdown pandas DataFrame...")
        query_concepts_index, _ = pd.MultiIndex.from_product([self._sorted_all_queries, self._sorted_all_concept_numbers, ["duration", "span"]], names=['query', 'concepts', 'duration_spanobject']).sortlevel() # sorted index is faster
        self._toplevel_query_breakdown = pd.DataFrame([], columns=query_concepts_index, index=pd.RangeIndex(self._repetitions))
        print("...finished allocating toplevel_query_breakdown dataframe")


    def _fill_query_breakdown_data(self, num_concepts, queries):
        """ Fill in any missing data in self._toplevel_query_breakdown. Operates columnwise.
        num_concepts: int
        queries: str[]
        """
        print("Collecting query breakdown data")

        # fill in any missing data in self._toplevel_query_breakdown
        for query in queries:
            # this corresponds to a (query, num_concepts) bigcolumn in the toplevel_query_breakdown
            column = self._toplevel_query_breakdown[(query, num_concepts)]
            not_filled = column.isnull().values.any()
            if not_filled:
                # retrieve spanId that is the parent from the duration data
                batch_span_id = self._overview_data.loc[num_concepts, (query, "batchSpanId")]
                # retrieve all spans with this as parent
                query_spans = self._zipkinESStorage.get_spans_with_parent(batch_span_id)
                for query_span in query_spans:
                    # have to manually parse repetition into int since they're not sorted because ES isn't parsing longs correctly
                    repetition = int(query_span['tags']['repetition'])
                    self._toplevel_query_breakdown.loc[repetition, (query, num_concepts)] = [query_span['duration'], query_span]

    # --- end data functions ---


    # --- graph/visualisation functions ---


    def _get_overview_graph(self, value='all'):
        """ Renders the overview graph. Can be extended with a filter/dropdown of some sort (treated as a callback already) """

        print("BenchmarkExecutionComponent._get_overview_graph...")

        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint [DONE]
        # 2. Generate a bar graph based on this dataframe, possibly filtered [DONE]
        if value == "all":
            filtered_dataframe = self._overview_data
        # may be filtered to a specific set of columns
        elif type(value) == list:
            filtered_dataframe = self._overview_data[value] # list indexing!
        # may be filtered to one column (mostly for manual triggering)
        elif type(value) == str and value in self._overview_data:
            filtered_dataframe = self._overview_data[value]
        else:
            print("Unknown query filter value: {0}".format(value))
            return None
        bargraphs = self._dataframe_to_bars(filtered_dataframe)

        duration_graph = dcc.Graph(
            id='duration-data-{0}'.format(self._execution_number),
            figure={
                'data': bargraphs,
                'layout': go.Layout(
                    barmode='group',
                    title='Duration data'
                )
            }
        )

        return duration_graph

    def _get_query_breakdown_graphs(self, value='maxconcepts', queries='all'):
        """ Render the query breakdowns """
        print("BenchmarkExecutionComponent._get_query_breakdown_graphs")

        if value == 'maxconcepts':
            # find the largest concepts size
            concepts_to_plot = self._sorted_all_concept_numbers[-1]
        elif value in self._sorted_all_concept_numbers:
            # plot a specific
            concepts_to_plot = value
        else:
            print("Unrecognized query breakdown specification: {0}".format(value))
            return None
        if queries == 'all':
            queries_to_plot = self._sorted_all_queries
        elif type(queries) == list:
            queries_to_plot = queries
        elif type(queries) == str and queries in self._sorted_all_queries:
            queries_to_plot = [queries]
        else:
            print("Cannot handle query breakdowns for queries: {0}".format(queries))
            return None

        # now have concepts_to_plot: int, and queries_to_plot: str[]
        # self._query_breakdown_data gets reused via slicing
        self._fill_query_breakdown_data(concepts_to_plot, queries_to_plot)

        div = html.Div(
            children=[
                html.H3("Query Breakdowns -- {0} concepts".format(concepts_to_plot))
            ])

        # convert breakdown data into SpanList objects
        # TODO reuse SpanLists

        for query in queries_to_plot:

            query_number = self._sorted_all_queries.index(query)
            root_spanlists = self._get_spanlists(concepts_to_plot, query, split_repetitions_at=1)

            # create graphs for this query/concepts combination
            query_breakdown_container = html.Div(
                children=[
                    html.Hr(),
                    html.H5(query)
                ]
            )

            for (i, spanlist) in enumerate(root_spanlists):
                name = spanlist.get_assigned_name()
                if spanlist.get_num_rows() == 1:
                    style = 'bar'
                    layout_options = {'barmode': 'stack'}
                else:
                    style = 'box'
                    layout_options = {}
                spanlists_graph =  SpanListsListGraph(graph_name=name,
                                                      root_spanlists=[spanlist],
                                                      style=style,
                                                      layout_options=layout_options)

                graph_id = 'breakdown-graph-{0}-{1}-{2}'.format(self._execution_number, query_number, i)
                self._spanlists_graphs_id_map[graph_id] = spanlists_graph
                figure = spanlists_graph.get_figure()
                graph = dcc.Graph(
                    id=graph_id,
                    figure=figure
                )
                query_breakdown_container.children.append(graph)
            # add these graphs to the Query Breakdowns div
            div.children.append(query_breakdown_container)

        return div


    def _dataframe_to_bars(self, dataframe):
        """ Consumes a dataframe, columns indexed by 'query' then 'duration_spanid' which are alternating data columns """

        # extract only the columns that have the 'duration', ignoring the 'spans' columns
        # : implies do this for all rows
        # slice(None) means do this for each 'query' super-column
        duration_columns = dataframe.loc[:, (slice(None), ['duration'])]

        # get the queries index un-duplicated (super-column labels)
        query_labels = dataframe.columns.unique(0).tolist()
        graphs = []
        def generate_bars(xs, ys, label):
            return go.Bar(
                x=xs,
                y=ys,
                name=label
            )

        for row in dataframe.index:
            graphs.append(generate_bars(query_labels, duration_columns.loc[row].values, row))
        return graphs

    # --- end graphs/visualisation functions ---

    def _get_spanlists(self, concepts, query, split_repetitions_at=1):
        """ Generate 1 or two spanlists per N repetitions. Set split_repetitions_at=0 to not split """
        # TODO validate split_repetitions_at is valid on lower AND upper range
        spanlists = []
        first_split = self._toplevel_query_breakdown.loc[0:split_repetitions_at-1, (query, concepts)]
        second_split = self._toplevel_query_breakdown.loc[split_repetitions_at:, (query, concepts)]

        if first_split.size != 0:
            spanlists.append(SpanList(self._zipkinESStorage, first_split, name="Repetitions [0:{0})".format(split_repetitions_at)))
        if second_split.size != 0:
            spanlists.append(SpanList(self._zipkinESStorage, second_split, name="Repetitions [{0}:{1})".format(split_repetitions_at, self._repetitions)))

        return spanlists

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
