import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import elasticsearch_helper as es_helper
import datetime
import numpy as np
import pandas as pd


app = dash.Dash()
es_utility = es_helper.ElasticsearchUtility()

def get_sorted_executions(es):
    """ Obtain from elasticsearch the existing benchmarking executions and return sorted by date """
    existing_executions = es.get_all_execution_names()
    # split and sort by formatted date
    date_format = "%Y-%m-%d %H:%M"
    parser = lambda date_string: datetime.datetime.strptime(date_string, date_format) 
    pairs = [(parser(x[:x.find(':')+2]), x) for x in existing_executions]
    pairs.sort(reverse=True, key=lambda pair: pair[0])
    return [x[1] for x in pairs]

# obtain the existing executions and turn them into a radio button
sorted_executions = get_sorted_executions(es_utility)
existing_executions_radio = dcc.RadioItems(options=[{'label': x, 'value': x} for x in sorted_executions], value="Existing executions")


# 1. make a pandas dataframe with traceId/concepts vs query and duration as datapoint [DONE]
spans_for_execution = es_utility.get_spans_with_experiment_name(sorted_executions[0])
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

# create a multi index that lets use acccess
# in rows the number of concepts
# and in columns query, duration or traceID columns (last two alternate)
index = pd.MultiIndex.from_tuples(query_concepts_map.keys(), names=['query', 'duration_traceid'])
data = pd.DataFrame(query_concepts_map, columns=index)

# 2. Generate a graph based on this data of some sort [DONE]
def graph_durations(dataframe):
    duration_columns = dataframe.loc[:, (slice(None), ['duration'])]
    query_labels = dataframe.columns.unique(0).tolist() # get the queries index un-duplicated
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

duration_graph = dcc.Graph(
        id='Duration Data',
        figure={
            'data': graph_durations(data),
            'layout': go.Layout(
                barmode='group',
                title='Duration data'
                )
            }
        )



# 3. When needed, for a specific traceID (ie number of concepts + query + benchmark run combination),
#    create a DataFrame by querying for traces with this traceID as parent (=> repetitions)
#    create a repetitions vs direct children table (with more queries with parent span ID)
active_concepts = set(data.index.tolist()) # just take all for now
active_queries = data.columns.unique(0).tolist()

# want to form a new index of (queries, concepts, duration/spanobject)
# just use one big table and slice it respectively 
query_concepts_index = pd.MultiIndex.from_product([active_queries, active_concepts, ["duration", "span"]], 
        names=['query', 'concepts', 'duration_spanobject'])

# create an empty dataframe for this, preallocating the number of rows needed
toplevel_breakdown = pd.DataFrame([], columns=query_concepts_index, index=pd.RangeIndex(repetitions)) 


def spans_graph(spanlist):


    pass

    


# lets populate this large table
for query in active_queries:
    for concepts in active_concepts:
        # retrieve spanId that is the parent from the duration data
        batch_span_id = data.loc[concepts, (query, "batchSpanId")]
        # retrieve all spans with this as parent
        query_spans = es_utility.get_spans_with_parent(batch_span_id)
        for query_span in query_spans:
            # have to do it this way since they're not sorted because ES isn't parsing longs correctly
            repetition = int(query_span['tags']['repetition'])
            toplevel_breakdown.loc[repetition, (query, concepts)] = [query_span['duration'], query_span]







# slice this table into SpanLists for aggregation


test1 = [1.0, 10.0]
test2 = [2.0, 11.0]
test3 = [4.0, 13.0]
test4 = [5.0, 14.0]
xs = [1.0, 1.0]
test_candlestick = go.Candlestick(x=xs,open=test2,high=test4,low=test1,close=test3)
test_graph = dcc.Graph(
        id="candlestick test",
        figure={
            'data': test_candlestick,
            'layout': go.Layout(title="test candlestick")
            }
        )

open_data = [33.0, 33.3, 33.5, 33.0, 34.1]
high_data = [33.1, 33.3, 33.6, 33.2, 34.8]
low_data = [32.7, 32.7, 32.8, 32.6, 32.8]
close_data = [33.0, 32.9, 33.3, 33.1, 33.1]
dates = [datetime.datetime(year=2013, month=10, day=10),
         datetime.datetime(year=2013, month=11, day=10),
         datetime.datetime(year=2013, month=12, day=10),
         datetime.datetime(year=2014, month=1, day=10),
         datetime.datetime(year=2014, month=2, day=10)]

test_graph = go.Candlestick(x=dates,
                       open=open_data,
                       high=high_data,
                       low=low_data,
                       close=close_data)
test_graph = dcc.Graph(
        id="test",
        figure={
            'data': test_graph,
            'layout': go.Layout()
            }
        )


class SpanList(object):

    def __init__(self, es_utility, dataframe):
        """ Takes an [excerpt of] a dataframe. Rows are elements to aggregate on retrieval """
        self.es_utility = es_utility
        self.dataframe = dataframe

        # obtain grandchild names for reference
        self.child_names = self._retrieve_child_names()

        # compute commonalities to this set of spans
        spans = data['spans'].tolist() # get the raw _source dictionary
        all_data = []
        self.all_span_ids = []
        for span in spans:
            self.all_span_ids.append(span['id'])
            items = set([]) 
            for (key, value) in span.items():
                try:
                    items.add((key, value))
                except TypeError:
                    continue # not hashable type
            all_data.append(items)
        # perform intersection over all these sets to find common keys
        self.common_data = set.intersection(*all_data)


    def get_name(self):
        return self.common_data['name']

    def get_mean(self):
        """ Compute mean of these durations """
        return self.dataframe['duration'].mean()

    def get_stddev(self):
        """ Compute sample standard deviation of the duration column """
        return self.dataframe['duration'].std()
        

    def _retrieve_child_names(self):
        """ obtain the names of the children of the children for later use """
        if len(self.dataframe) == 0:
            return

        # retrieve first row
        a_child = self.dataframe.iloc[0]
        child_span_id = a_child['id']
        return set([self.es_utility.get_child_names(parent_id=child_span_id)])

   
    def get_child_spanlists(self):
        
        for child_name in self.child_names:
            child_spans = self.es_utility.get_named_span_with_parents(child_name,
                                            self.all_span_ids)

            # create the dataframe
            data = {
                "duration": [int(span['duration']) for span in child_spans],
                "spans": child_spans
                }
            dataframe = pd.DataFrame(data)

            return SpanList(self.es_utility, dataframe)



















#  => this also can be used to calculate query timing coverage
# 4. On request to drill down, create new data frame with the selected span ID as parent
# These dataframes can be sliced by repetition easily
# 



sidebar = html.Div(children=[
    html.H3("Benchmarks"),
    existing_executions_radio
    ], className="sidebar")

app.layout = html.Div(children=[
    html.H1("Grakn Benchmarking Dashboard"),
    sidebar,
    duration_graph,
    test_graph 
    ])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(debug=True)
