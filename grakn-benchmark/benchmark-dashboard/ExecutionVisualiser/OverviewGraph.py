import dash_core_components as dcc
import plotly.graph_objs as go

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
