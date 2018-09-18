import dash_core_components as dcc
import plotly.graph_objs as go

from ExecutionVisualiser.SpansDataCollection import SpansDataCollection

class BreakdownGraph(object):

    def __init__(self, graph_name, graph_id, partitioned_root_spans_data, x_axis_divisor=1000.0):
        self._graph_name = graph_name
        self._graph_id = graph_id
        self._x_axis_divisor = x_axis_divisor
        self._partitioned_root_spans_data = partitioned_root_spans_data

        # pack the toplevel SpansData into a unit length SpansDataCollection
        root_spans_data_collection = SpansDataCollection(label="Root")
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
