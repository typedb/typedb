import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import numpy as np
import pandas as pd 
from SpanList import SpanList


def inorder(l, reverse=False):
    if not reverse:
        return all(a<=b for (a,b) in zip(l, l[1:]))
    else:
        return all(b<=a for (a,b) in zip(l, l[1:]))

def string_to_html_id(s):
    return s.replace(" ", '-').replace(':', '_').replace(";",'_').replace("\"", "_").replace("$", "-")

class BenchmarkExecutionComponent(object):

    def __init__(self, app, es_utility, execution_name, unique_number):
        print("Creating BenchmarkExecutionComponent...")

        self.app = app
        self.es_utility = es_utility
        self.execution_name = execution_name.strip()
        self.unique_number = unique_number

        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint
        # note: this builds for all the queries right away since this isn't that expensive
        self.overview_data = None
        self.repetitions = None         
        self._build_overview_data()
        # the indexes are pre-sorted for effiency
        self.sorted_all_concept_numbers = self.overview_data.index.unique().tolist()
        self.sorted_all_queries = self.overview_data.columns.unique(0).tolist()
        assert inorder(self.sorted_all_concept_numbers), "Concept numbers not sorted!"
        assert inorder(self.sorted_all_queries), "Queries not sorted!"
        self.toplevel_query_breakdown = None
        self._allocate_toplevel_query_breakdown()

        print("...finished creating BenchmarkExecutionComponent")


    @staticmethod
    def get_required_callback_definitions(unique_number, graph_callbacks=100):
        rendering_callbacks = {
            '_render_overview': (
                dash.dependencies.Output("overview-{0}".format(unique_number), "children"),
                [
                    dash.dependencies.Input("query-selector-{0}".format(unique_number), "value")
                ]
            ),
            '_render_query_breakdown': (
                dash.dependencies.Output("query-breakdowns-{0}".format(unique_number), "children"),
                [
                    dash.dependencies.Input("concepts-radio-{0}".format(unique_number), "value"),
                    dash.dependencies.Input("query-selector-{0}".format(unique_number), "value")
                ]
            )
        }

        # graph_calbacks = {}
        # for i in range(graph_callbacks):
        #     # `i` corresponds t
        #     graph_callbacks

        return rendering_callbacks

    def route_callback(self, method_name, *args):
        if method_name == '_render_overview':
            return self._render_overview(*args)
        elif method_name == '_render_query_breakdown':
            return self._render_query_breakdown(*args)
        else:
            print("Unknown method name in route_callback: {0}".format(method_name))


    def _allocate_toplevel_query_breakdown(self):
        print("Begin allocating toplevel_query_breakdown pandas DataFrame...")
        query_concepts_index, _ = pd.MultiIndex.from_product([self.sorted_all_queries, self.sorted_all_concept_numbers, ["duration", "span"]], names=['query', 'concepts', 'duration_spanobject']).sortlevel() # sorted index is faster
        print("self.repetitions: {0}".format(self.repetitions))
        self.toplevel_query_breakdown = pd.DataFrame([], columns=query_concepts_index, index=pd.RangeIndex(self.repetitions)) 
        print("...finished allocating toplevel_query_breakdown dataframe")


    def full_render(self):
        print("BenchmarkExecutionComponent.full_render")

        query_selector = dcc.Dropdown(
            id='query-selector-{0}'.format(self.unique_number),
            options=[{'label':q, 'value':q} for q in self.sorted_all_queries],
            value=self.sorted_all_queries,
            multi=True
        )
        concepts_selector = dcc.RadioItems(
            id='concepts-radio-{0}'.format(self.unique_number),
            options=[{'label': n, 'value': n} for n in self.sorted_all_concept_numbers],
            value=self.sorted_all_concept_numbers[-1]
        )

        rendered = html.Div(
            id='component-{0}'.format(self.unique_number),
            children=[
                html.H5("Repetitions: {0}".format(self.repetitions)),
                html.Div(
                    children=[
                        html.H5("Queries executed"),
                        query_selector
                    ]
                ),
                html.Div(
                    id='overview-{0}'.format(self.unique_number)
                ),
                html.Div(children=[
                    html.H5("Number of concepts benchmarked"),
                    concepts_selector
                    ]
                ),
                html.Div(
                    id='query-breakdowns-{0}'.format(self.unique_number)
                )
            ]
        )

        return rendered

    def _render_overview(self, value='all'):
        """ Renders the overview graph. Can be extended with a filter/dropdown of some sort (treated as a callback already) """
        
        print("BenchmarkExecutionComponent._render_overview")

        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint [DONE]
        # 2. Generate a bar graph based on this dataframe, possibly filtered [DONE]
        if value == "all":
            filtered_dataframe = self.overview_data
        # may be filtered to a specific set of columns
        elif type(value) == list:
            filtered_dataframe = self.overview_data[value] # list indexing!
        # may be filtered to one column (mostly for manual triggering)
        elif type(value) == str and value in self.overview_data:
            filtered_dataframe = self.overview_data[value]
        else:
            print("Unknown query filter value: {0}".format(value))
            return None
        bargraphs = self._dataframe_to_bars(filtered_dataframe)
        
        duration_graph = dcc.Graph(
            id='duration-data-{0}'.format(self.unique_number),
            figure={
                'data': bargraphs,
                'layout': go.Layout(
                    barmode='group',
                    title='Duration data'
                    )
                }
            )
        return duration_graph 


    def _render_query_breakdown(self, value='maxconcepts', queries='all'):
        """ Render the query breakdowns """
        print("BenchmarkExecutionComponent._render_query_breakdown")

        print("Rendering query breakdown")

        # call we pass lists through the HTML callback?
        if value == 'maxconcepts':
            # find the largest concepts size
            concepts_to_plot = self.sorted_all_concept_numbers[-1]
        elif value in self.sorted_all_concept_numbers:
            # plot a specific 
            concepts_to_plot = value
        else:
            print("Unrecognized query breakdown specification: {0}".format(value))
            return None
        if queries == 'all':
            queries_to_plot = self.sorted_all_queries
        elif type(queries) == list:
            queries_to_plot = queries
        elif type(queries) == str and queries in self.sorted_all_queries:
            queries_to_plot = [queries]
        else:
            print("Cannot handle query breakdowns for queries: {0}".format(queries))
            return None
    
        # now have concepts_to_plot: int, and queries_to_plot: str[]
        # self.query_breakdown_data gets reused via slicing
        self._fill_query_breakdown_data(concepts_to_plot, queries_to_plot)

        div = html.Div(
            children=[
                html.H3("Query Breakdowns -- {0} concepts".format(concepts_to_plot))
            ])

        # convert breakdown data into SpanList objects
        # TODO reuse SpanLists

        for query in queries_to_plot:
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
                graph =  SpanListsListGraph(graph_name=name,
                                            root_spanlists=[spanlist],
                                            style=style,
                                            layout_options=layout_options)
                query_breakdown_container.children.append(graph.get_figure("breakdown-{0}-{1}-{2}".format(self.unique_number, string_to_html_id(query), i), self.app))
            # add these graphs to the Query Breakdowns div
            div.children.append(query_breakdown_container)

        return div



    def _get_spanlists(self, concepts, query, split_repetitions_at=1):
        """ Generate 1 or two spanlists per N repetitions. Set split_repetitions_at=0 to not split """
        # TODO validate split_repetitions_at is valid on lower AND upper range
        spanlists = []
        first_split = self.toplevel_query_breakdown.loc[0:split_repetitions_at-1, (query, concepts)]
        second_split = self.toplevel_query_breakdown.loc[split_repetitions_at:, (query, concepts)]

        if first_split.size != 0:
            spanlists.append(SpanList(self.es_utility, first_split, name="Repetitions [0:{0})".format(split_repetitions_at)))
        if second_split.size != 0:
            spanlists.append(SpanList(self.es_utility, second_split, name="Repetitions [{0}:{1})".format(split_repetitions_at, self.repetitions)))

        return spanlists


    def _fill_query_breakdown_data(self, num_concepts, queries):
        """ Fill in any missing data in self.toplevel_query_breakdown. Operates columnwise.
        num_concepts: int
        queries: str[]
        """
        print("Collecting query breakdown data")

        # fill in any missing data in self.toplevel_query_breakdown
        for query in queries:
            # this corresponds to a (query, num_concepts) bigcolumn in the toplevel_query_breakdown
            column = self.toplevel_query_breakdown[(query, num_concepts)]
            not_filled = column.isnull().values.any()
            if not_filled:
                # retrieve spanId that is the parent from the duration data
                batch_span_id = self.overview_data.loc[num_concepts, (query, "batchSpanId")]
                # retrieve all spans with this as parent
                query_spans = self.es_utility.get_spans_with_parent(batch_span_id)
                for query_span in query_spans:
                    # have to manually parse repetition into int since they're not sorted because ES isn't parsing longs correctly
                    repetition = int(query_span['tags']['repetition'])
                    self.toplevel_query_breakdown.loc[repetition, (query, num_concepts)] = [query_span['duration'], query_span]


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


    def _build_overview_data(self):
        print("Building overview data...")
        spans_for_execution = self.es_utility.get_spans_with_experiment_name(self.execution_name)
        query_concepts_map = {} # collect data into a map
        for span in spans_for_execution:
            query = span['tags']['query']
            if self.repetitions is None:
                self.repetitions = int(span['tags']['repetitions'])
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
        self.overview_data = pd.DataFrame(query_concepts_map, columns=index)
        print("...finished building overview data") 


testing_inserted = False

class SpanListsListGraph(object):
    """ TODO convert this to pandas dataframe to avoid recomputing? """

    def __init__(self, graph_name, root_spanlists, style='box', layout_options={}):
        self.graph_name = graph_name
        self.style = style
        self.layout_options = layout_options

        self.levels = {}
        self.levels[0] = {
           'spanlists': root_spanlists,
           'data': self._spanlists_to_plot_definition("Root Span", root_spanlists, style=self.style),
           'descendsfrom': 0 # used to avoid deleting stuff that is arleady computed
        }
        
        self._expand_single_spans()

    def _expand_single_spans(self):
        """ If the deepest spanlist[] only has 1 spanlist, automatically expand it """
        max_level, num_spans = self.get_max_level_and_num_spans()
        while num_spans == 1:
            next_level_name = "Level {0}".format(max_level+1)
            max_level_spanlists = self.levels[max_level]['spanlists']
            # compute the child spanlists
            children = max_level_spanlists[0].get_child_spanlists()
            next_level_data = self._spanlists_to_plot_definition(next_level_name, children, style=self.style)
            self.levels[max_level+1] = {
                'spanlists': children,
                'data': next_level_data,
                'descendsfrom': 0 # auto-expand is always child number 0
            }
            
            max_level, num_spans = self.get_max_level_and_num_spans()


    def get_plot_data(self):
        data = []
        for level in self.levels:
            data += self.levels[level]['data']
        return data
        

    def get_current_max_level(self):
        return max(self.levels.keys())


    def get_max_level_and_num_spans(self):
        max_level = self.get_current_max_level()
        return max_level, len(self.levels[max_level]["spanlists"])


    def selectable_spans_at_level(self, level):
        spanlists = self.levels[level]["spanlists"]
        names = [(i, spanlist.get_spans_name()) for i, spanlist in enumerate(spanlists)]
        return names

    def expand_span_at_level(self, level_number, spannumber, spanname):
        # TODO this can be made more efficient with a look up from childname to spanlists if needed
        
        # first clear out the data that is stored
        if level_number not in self.levels:
            raise Exception("Cannot expand a level that isn't current computed")

        level = self.levels[level_number]
        if level_number+1 in self.levels:
            next_level = self.levels[level_number+1]
            if spannumber == next_level['descendsfrom']:
                # TODO what to do here if matches computed value in terms of rendering
                return # nothing to update
        
        # delete all further levels down 
        for i in range(level+1, self.get_current_max_level()):
            del self.levels[i]

        # recompute level_number + 1 
        spanlist = level['spanlists'][spannumber]
        children = spanlist.get_child_spanlists()
        self.levels[level_number + 1] = {
                'spanlists': children,
                'data': self._spanlists_to_plot_definition("Level {0}".format(level_number+1), children, style=self.style),
                'descendsfrom': spannumber
            }

        # expand further if there's only 1 span
        self._expand_single_spans()

        # TODO, what to do here, return new get_plot_data? How to hook into callback system



    def get_figure(self, id, app):
        id = string_to_html_id(id)
        layout = go.Layout(
                boxmode='group',
                xaxis={
                    'title': self.graph_name,
                    'zeroline': True
                },
                yaxis={
                    'autorange': 'reversed',
                    'type': 'category'
                },
                **self.layout_options
            )

        figure = dcc.Graph(
                    id=id,
                    figure={
                        'data': self.get_plot_data(),
                        'layout': layout
                    })

        global testing_inserted
        if not testing_inserted:
            print("!!!!! Adding callback for html id: {0}".format(id))
            self.figure_clicked = app.callback(
                dash.dependencies.Output('testing-output', 'children'),
                [dash.dependencies.Input(id, 'clickData')]
            )(self.figure_clicked)
            testing_inserted = True

        return figure

    def figure_clicked(self, *values):
        print(values)


    def _spanlists_to_plot_definition(self, category, spanlists, style='box'):
        """ SpanList[] to definitions of box or bar plot """
        data = []
        for spanlist in spanlists:
            x_data = spanlist.get_values_np()
            data.append({
                "x" : x_data,
                "y" : [category] * x_data.shape[0],
                "name" : spanlist.get_spans_name(),
                "boxmean": True,
                "orientation": 'h',
                "type": style
                })
        return data
