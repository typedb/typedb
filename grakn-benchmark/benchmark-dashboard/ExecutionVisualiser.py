import dash
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
import plotly.graph_objs as go
import abc
import numpy as np

from collections import Counter

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
        self._query_selector = ExecutionVisualiser.QuerySelector(self._execution_number, self._sorted_queries)
        self._concept_count_selector = ExecutionVisualiser.ConceptCountSelector(self._execution_number,
                                                                                self._sorted_concept_counts)
        self._overview_graph = ExecutionVisualiser.OverviewGraph(self._execution_number, self._overview_dataframe)

        # initially empty container for storing the toplevel queries' data, used for aggregation and drill down
        self._root_spans_data = ExecutionVisualiser.FullRootSpansData(
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
            return self._route_on_graph_click_callback(method_name, *args)
        else:
            print("Unknown method name in route_callback: {0}".format(method_name))

    def _route_on_graph_click_callback(self, method_name, *args):
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

            # one click could hit multiple levels or curves, find majority
            levels_clicked = [pt['y'] for pt in clicked_points]
            level_counts = Counter(levels_clicked)
            most_common_level = level_counts.most_common(1)[0][0]

            curves_clicked = [pt['curveNumber'] for pt in clicked_points]
            curve_counts = Counter(curves_clicked)
            most_common_curve_number = curve_counts.most_common(1)[0][0]
            # convert overall curve number into an offset AT a level
            (_, level_span_number) = breakdown_graph.curve_number_to_level_and_child_tuple(most_common_curve_number)

            # update the spanlists graph!
            print("Expanding graph at: {0}, {1}".format(most_common_level, level_span_number))
            breakdown_graph.expand_level_name(most_common_level, level_span_number)
            return breakdown_graph.get_figure()
        else:
            return {}

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

    def get_overview_graph(self, *args):
        return self._overview_graph.get_layout()

    class OverviewGraph(object):

        def __init__(self, execution_number, overview_dataframe):
            self._execution_number = execution_number
            self._overview_dataframe = overview_dataframe

        def get_layout(self):
            return self._to_grouped_bar_graph()

        def _to_grouped_bar_graph(self, value='all'):
            """ Renders the overview graph. Can be extended with a filter/dropdown of some sort (treated as a callback already) """

            print("OverviewGraph._to_grouped_bar_graph...")

            # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint [DONE]
            # 2. Generate a bar graph based on this dataframe, possibly filtered [DONE]
            if value == "all":
                filtered_dataframe = self._overview_dataframe
            # may be filtered to a specific set of columns
            elif type(value) == list:
                filtered_dataframe = self._overview_dataframe[value] # list indexing!
            # may be filtered to one column (mostly for manual triggering)
            elif type(value) == str and value in self._overview_dataframe:
                filtered_dataframe = self._overview_dataframe[value]
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


    class FullRootSpansData(object):

        def __init__(self, zipkin_ES_storage, overview_data_ref, sorted_queries, sorted_concept_counts, repetitions):
            print("Begin creating RootSpansData")
            self._zipkin_ES_storage = zipkin_ES_storage
            self._overview_data_ref = overview_data_ref
            query_concepts_index, _ = pd.MultiIndex.from_product([sorted_queries, sorted_concept_counts, ["duration", "span"]], names=['query', 'concepts', 'duration_spanobject']).sortlevel() # sorted index is faster
            self._root_spans_dataframe = pd.DataFrame([], columns=query_concepts_index, index=pd.RangeIndex(repetitions))
            print("...finished creating and allocating RootSpansData pandas dataframe")

        def upsert_breakdown_for_concepts_and_queries(self, concept_count, queries):
            """ Fill in any missing data in self._toplevel_query_breakdown. Operates columnwise.
            num_concepts: int
            queries: str[]
            """
            print("Collecting query breakdown data")

            # fill in any missing data in self._toplevel_query_breakdown
            for query in queries:
                # this corresponds to a (query, num_concepts) bigcolumn in the toplevel_query_breakdown
                column = self._root_spans_dataframe[(query, concept_count)]
                not_filled = column.isnull().values.any()
                if not_filled:
                    # retrieve spanId that is the parent from the duration data
                    batch_span_id = self._overview_data_ref.loc[concept_count, (query, "batchSpanId")]
                    # retrieve all spans with this as parent
                    query_spans = self._zipkin_ES_storage.get_spans_with_parent(batch_span_id)
                    for query_span in query_spans:
                        # have to manually parse repetition into int since they're not sorted because ES isn't parsing longs correctly
                        repetition = int(query_span['tags']['repetition'])
                        self._root_spans_dataframe.loc[repetition, (query, concept_count)] = [query_span['duration'], query_span]



        def partition_for_query_and_concept_count(self, query, concept_count, partition_indices=[1]):
            """
            Split the RootSpansData into sub-sections and returns graphs (ie split rows into chunks).
            Splits UP TO the next index
            """

            root_spans_data_partitions = []
            start_index = 0
            end_index = self._root_spans_dataframe.shape[0] # number of rows total
            partition_indices += [end_index] # edit a copy in place going to the end of the rows
            for index in partition_indices:
                partition = self._root_spans_dataframe.loc[start_index:index-1, (query, concept_count)]
                root_spans_data_partitions.append(
                    ExecutionVisualiser.PartitionedRootSpansData(
                        name="Partition: row {0} to {1}".format(start_index, index),
                        partitioned_dataframe=partition,
                        zipkin_ES_storage=self._zipkin_ES_storage
                    )
                )
                start_index = index
            return root_spans_data_partitions



    class SpansData(object):
        def __init__(self, name, dataframe, zipkin_ES_storage):
            self._assigned_name = name
            self._dataframe = dataframe
            self._zipkin_ES_storage = zipkin_ES_storage
            self._span_ids = self._get_span_ids()
            self._common_data = self._compute_common_data()

        def get_num_rows(self):
            return self._dataframe.shape[0]

        def get_assigned_name(self):
            return self._assigned_name

        def get_spans_name(self):
            return self._common_data['name']

        def get_repetition_count(self):
            return len(self._span_ids)

        def get_number_of_children(self):
            an_id = self._span_ids[0]
            return self._zipkin_ES_storage.get_number_of_children(parent_id=an_id)

        @abc.abstractmethod
        def _get_span_ids(self):
            pass

        @abc.abstractmethod
        def _compute_common_data(self):
            pass

        @abc.abstractmethod
        def get_values_np(self):
            pass

        @staticmethod
        def _compute_common_span_data(spans):
            all_data = []
            for span in spans:
                items = set([])
                for (key, value) in span.items():
                    try:
                        items.add((key, value))
                    except TypeError:
                        continue # not hashable type
                all_data.append(items)
            # perform intersection over all these sets to find common keys
            return dict(set.intersection(*all_data))

        def get_child_spans_data_collection(self):
            repetitions = self.get_repetition_count()
            columns_index = pd.RangeIndex(repetitions)
            number_of_children = self.get_number_of_children()
            rows_index = pd.MultiIndex.from_product([np.arange(number_of_children), ["duration", "span"]], names=["orderedchild", "duration_spanobject"])
            child_data = pd.DataFrame(index=rows_index, columns=columns_index)
    
            # This has been rewritten to do the following:
            # because not every child has a unique name
            # we need to retrieve all children of a parentID, sorted by timestamp/SOMETHING ELSE TODO
            # these are inserted into a dataframe
            # once we have a table of children vs repetition,
            # take columns of the table as SpanLists
    
            for i, parent_id in enumerate(self._get_span_ids()):
                sorted_child_spans = self._zipkin_ES_storage.get_spans_with_parent(parent_id, sorting={"timestamp": "asc"})
                durations = [span['duration'] for span in sorted_child_spans]
                child_data.loc[(slice(None), "duration"), i] = durations
                child_data.loc[(slice(None), "span"), i] = sorted_child_spans

            child_spans_data_collection = ExecutionVisualiser.SpansDataCollection(
                label="children of {0}".format(self.get_spans_name())
            )
    
            # take Transpose, then use columns to create new spanlists
            child_data = child_data.T
            for col in child_data.columns.levels[0].unique():
                column_data = child_data.loc[:, col]
                counts = Counter(x['name'] for x in column_data.loc[:, "span"])
    
                # TODO short term hack to ignore out of order rows by voting
                argmax = counts.most_common(1)[0][0]
                matches = [True if span['name'] == argmax else False for span in column_data.loc[:, "span"]]
                matching_rows = column_data.loc[matches]
    
                if len(counts) > 1:
                    print(counts)
                    print("HELP! Out of order sorting??")
                    print("Fixed by ignoring by voting")
                    print("TODO fix elasticsearch properly...")

                child_spans_data = ExecutionVisualiser.ChildSpansData(
                    name="child",
                    dataframe=matching_rows,
                    zipkin_ES_storage=self._zipkin_ES_storage
                )
                child_spans_data_collection.add_spans_data(child_spans_data)

            return child_spans_data_collection


    class SpansDataCollection(object):

        def __init__(self, label):
            self._spans_data_list = []
            self._label = label
            self.descends_from = None

        def add_spans_data(self, spans_data):
            self._spans_data_list.append(spans_data)

        def get_child_spans_data_collection_of(self, children_of=0):
            child_spans_data_collection = self._spans_data_list[children_of].get_child_spans_data_collection()
            child_spans_data_collection.descends_from = children_of
            return child_spans_data_collection

        def iterator(self):
            return iter(self._spans_data_list)

        def get_label(self):
            return self._label

        def get_size(self):
            return len(self._spans_data_list)




    class PartitionedRootSpansData(SpansData):
        """ Specialise SpansData object for root level SpanData, that have their own indices """

        def __init__(self, name, partitioned_dataframe, zipkin_ES_storage):
            super().__init__(name, partitioned_dataframe, zipkin_ES_storage)


        def _get_span_ids(self):
            """ Override abstract, access internal dataframe indices and return all span IDS """
            # spans = self._dataframe.xs('span', level='duration_spanobject', axis=1) # get the raw _source dictionaries
            spans = self._dataframe['span']
            spans = spans.values.ravel()
            span_ids = [span['id'] for span in spans]
            return span_ids

        def _compute_common_data(self):
            """ Override abstract, access internal dataframe => spans, compute set of common attributes of spans """
            # spans = self._dataframe.xs('span', level='duration_spanobject', axis=1) # get the raw _source dictionaries
            spans = self._dataframe['span']
            spans = spans.values.ravel()
            return ExecutionVisualiser.SpansData._compute_common_span_data(spans)

        def get_values_np(self):
            """ Override abstract, access internal dataframe => durations as flat numpy array """
            return self._dataframe['duration'].values.ravel()
            # return self._dataframe.xs('duration', level='duration_spanobject', axis=1).values.ravel()




    class ChildSpansData(SpansData):
        """ Specialise SpansData object for child spans, which have simpler indices """

        def __init__(self, name, dataframe, zipkin_ES_storage):
            super().__init__(name, dataframe, zipkin_ES_storage)

        def _get_span_ids(self):
            """ Override abstract, access internal dataframe indices and return all span IDS """
            spans = self._dataframe["span"]
            spans = spans.values.ravel()
            span_ids = [span['id'] for span in spans]
            return span_ids

        def _compute_common_data(self):
            """ Override abstract, access internal dataframe => spans, compute set of common attributes of spans """
            spans = self._dataframe["span"]
            spans = spans.values.ravel()
            return ExecutionVisualiser.SpansData._compute_common_span_data(spans)


        def get_values_np(self):
            """ Override abstract, access internal dataframe => spans, compute set of common attributes of spans """
            return self._dataframe['duration'].values.ravel()




    class BreakdownGraph(object):

        def __init__(self, graph_name, graph_id, partitioned_root_spans_data, x_axis_divisor=1000.0):
            self._graph_name = graph_name
            self._graph_id = graph_id
            self._x_axis_divisor = x_axis_divisor
            self._partitioned_root_spans_data = partitioned_root_spans_data

            # pack the toplevel SpansData into a unit length SpansDataCollection
            root_spans_data_collection = ExecutionVisualiser.SpansDataCollection(label="Root")
            root_spans_data_collection.add_spans_data(partitioned_root_spans_data)
            self.levels = [root_spans_data_collection]
            self._expand_single_spans()

            if self._partitioned_root_spans_data.get_num_rows() == 1:
                self._style = 'bar'
                self._layout_options = {
                    'barmode': 'stack'
                }
            else:
                self._style = 'box'
                self._layout_options = {}



        def get_layout(self):
            graph = dcc.Graph(
                id=self._graph_id,
                figure=self.get_figure()
            )
            return graph

        def get_figure(self):
            """ Update the graph `figure` with new data, rather than re-rendering entire Graph component """

            layout = go.Layout(
                title=self._graph_name,
                boxmode='group',
                xaxis={
                    'title': 'milliseconds (ms)',
                    'zeroline': True
                },
                yaxis={
                    'autorange': 'reversed',
                    'type': 'category'
                },
                margin=go.layout.Margin(
                    l=150,
                    r=50,
                    b=50,
                    t=50,
                    pad=4
                ),
                **self._layout_options
            )

            figure = {
                'data': self._get_plot_data(),
                'layout': layout
            }

            return figure

        def _get_plot_data(self):
            data = []
            for spans_data_collection in self.levels:
                data += self._spans_data_collection_to_plot_data(spans_data_collection)
            return data

        def _spans_data_collection_to_plot_data(self, spans_data_collection):
            plot_data = []

            for spans_data in spans_data_collection.iterator():
                x_data = spans_data.get_values_np()
                plot_data.append({
                    "x" : x_data/self._x_axis_divisor,
                    "y" : [spans_data_collection.get_label()] * x_data.shape[0],
                    "name" : spans_data.get_spans_name(),
                    "boxmean": True,
                    "orientation": 'h',
                    "type": self._style
                })

            return plot_data


        def _expand_single_spans(self):
            """ While there is only 1 SpansData in the collection expand the final collection """
            while self.levels[-1].get_size() == 1:
                child_spans_data_collection = self.levels[-1].get_child_spans_data_collection_of(children_of=0)
                self.levels.append(child_spans_data_collection)

        def curve_number_to_level_and_child_tuple(self, curve_number):
            """ Count the total number of SpanData's plotted across levels and find the right level/span number """
            total_count = 0
            for level_number, spans_data_collection in enumerate(self.levels):
                collection_count = spans_data_collection.get_size()
                if total_count + collection_count > curve_number:
                    spans_data_index = curve_number - total_count
                    return (level_number, spans_data_index)
                total_count += collection_count

            raise Exception("Curve number {0} not found".format(curve_number))



        def expand_level_name(self, level_name, span_number):
            for level_number, spans_data_collection in enumerate(self.levels):
                if level_name == spans_data_collection.get_label():
                    self.expand(level_number, span_number)

        def expand(self, level_number, span_number):
            if span_number >= len(self.levels):
                raise Exception("Cannot expand level that isn't currently computed: {0} not one of levels {1}".format(
                    span_number, range(len(self.levels))
                ))

            # delete levels below the one we want to expand
            self._clear_levels_below(level_number)

            # compute level_number + 1
            child_spans_data_collection = self.levels[level_number].get_child_spans_data_collection_of(children_of=span_number)
            self.levels.append(child_spans_data_collection)

            self._expand_single_spans()

        def _clear_levels_below(self, span_number):
            # delete all levels below this one in reverse (because using a list for levels)
            for i in range(len(self.levels)-1, span_number, -1):
                del self.levels[i]


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
            query_number = self._sorted_queries.index(query)

            # partition the root spans table into sub tables
            partitioned_root_spans_data_list = self._root_spans_data.partition_for_query_and_concept_count(query, concept_count, partition_indices=[1])
            query_breakdown_container = html.Div(
                children=[
                    html.Hr(),
                    html.H5(query)
                ]
            )
            for (i, root_spans_data) in enumerate(partitioned_root_spans_data_list):
                graph_id = 'breakdown-graph-{0}-{1}-{2}'.format(self._execution_number, query_number, i)
                breakdown_graph = ExecutionVisualiser.BreakdownGraph(
                    graph_name=root_spans_data.get_assigned_name(),
                    graph_id=graph_id,
                    partitioned_root_spans_data=root_spans_data
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
