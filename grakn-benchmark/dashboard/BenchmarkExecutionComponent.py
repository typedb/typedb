import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import numpy as np
import pandas as pd 



class BenchmarkExecutionComponent(object):

    def __init__(self, app, es_utility, execution_name):
        self.app = app
        self.es_utility = es_utility
        self.execution_name = execution_name


        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint
        # note: this builds for all the queries right away since this isn't that expensive
        self._build_overview_data()
        self.toplevel_query_breakdown = None
        self.repetitions = None         # TODO fill this
        self._allocate_toplevel_query_breakdown()


        


        """

        Next steps:
            Fill out toplevel_query_breakdown when requested
            plot
            Test decorators
        """






    def _allocate_toplevel_query_breakdown(self):
        concept_numbers = set(self.overview_data.index.tolist())
        queries = self.overview_data.columns.unique(0).tolist()
        query_concepts_index, _ = pd.MultiIndex.from_product([sorted(active_queries), active_concepts, ["duration", "span"]], 
            names=['query', 'concepts', 'duration_spanobject']).sortlevel() # sorted index is faster
        self.toplevel_query_breakdown = pd.DataFrame([], columns=query_concepts_index, index=pd.RangeIndex(repetitions)) 


    def selfname_dashcallback(output, inputs):
        """ Append a unique part to the given input/output names """
        print("calling outer decorator!")
        print(output, inputs)
        def middle_callback(decorated):
            """ Takes in the decorated function """
            print("Called middle callback!")
            print(decorated)
            def exec_dash_callback(self, *args):
                """ Takes self and the function's original arguments """
                print("called inner callback!")
                print(self, args)
                dash_output = dash.dependencies.Output(output[0]+self.execution_name, output[1])
                dash_inputs = []
                for input in inputs:
                    dash_inputs.append(dash.dependencies.Input(input[0]+self.execution_name, input[1]))
                # act on the decorator directly
                return self.app.callback(dash_output, dash_inputs)(decorated)(self, *args)
            return exec_dash_callback
        return middle_callback


    def full_render(self):
        return html.Div(
            id='component-{0}'.format(self.execution_name),
            children=[
                html.Div(
                    id='overview-{0}'.format(self.execution_name),
                    value='all'
                ),
                html.Div(
                    id='query-breakdowns-{0}'.format(self.execution_name),
                    value='maxconcepts'
                )
            ]
        )


    @selfname_dashcallback(
        output=("overview-", "children"),
        inputs=[("overview-", "value")]
    )
    def _render_overview(self, value="all"):
        """ Renders the overview graph. Can be extended with a filter/dropdown of some sort (treated as a callback already) """
        
        # 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint [DONE]
        # 2. Generate a bar graph based on this dataframe, possibly filtered [DONE]
        if value == "all":
            filtered_dataframe = self.overview_data
        # may be filtered to a specific column
        elif value in self.overview_data:
            filtered_dataframe = self.overview_data[value]
        else:
            print("Unknown query filter value: {0}".format(value))
            return None
        bargraphs = self._dataframe_to_bars(filtered_dataframe)
        
        duration_graph = dcc.Graph(
            id='duration-data-{0}'.format(self.execution_name),
            figure={
                'data': bargraphs,
                'layout': go.Layout(
                    barmode='group',
                    title='Duration data'
                    )
                }
            )
        return duration_graph 


    @selfname_dashcallback(
        input=("query-breakdowns-", "value"),
        outputs=[("query-breakdowns-", "children")]
    )
    def _render_query_breakdown(self, value='maxconcepts'):
        """ Render the query breakdowns """


        i

        # call we pass lists through the HTML callback?
        if value == 'max':
            # find the largest concepts size
        elif type(value) == List):
            # find the ones to plot and plot those sequentially
        else:
            print("Unrecognized query breakdown specification: {0}".format(value))
            return None


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
        repetitions = None      # store how many repetitions each query has been executed for, for later use
        for span in spans_for_execution:
            query = span['tags']['query']
            if repetitions is None:
                repetitions = int(span['tags']['repetitions'])
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
        

        
