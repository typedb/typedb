import plotly.graph_objs as go

class SpanListsListGraph(object):
    """ TODO how to avoid recomputing child spanlists? """

    def __init__(self, graph_name, root_spanlists, style='box', layout_options={}):
        self.graph_name = graph_name
        self.style = style
        self.layout_options = layout_options

        self.levels = {}
        root_span_name = "Root Span"
        self.levels[0] = {
            'spanlists': root_spanlists,
            'data': self._spanlists_to_plot_definition(root_span_name, root_spanlists, style=self.style),
            'descendsfrom': 0 # used to avoid deleting stuff that is arleady computed
        }
        self.level_names = [root_span_name]

        self._expand_single_spans()


    def _expand_single_spans(self):
        """ If the deepest spanlist[] only has 1 spanlist, automatically expand it """
        max_level, num_spans = self.get_max_level_and_num_spans()
        while num_spans == 1:
            max_level_spanlists = self.levels[max_level]['spanlists']
            # compute the child spanlists
            spanlist = max_level_spanlists[0]
            children = spanlist.get_child_spanlists()
            displayname = "Level {0}:<br>children of<br> {1}".format(max_level+1, spanlist.get_spans_name())
            next_level_data = self._spanlists_to_plot_definition(displayname, children, style=self.style)
            self.levels[max_level+1] = {
                'spanlists': children,
                'data': next_level_data,
                'descendsfrom': 0 # auto-expand is always child number 0
            }
            self.set_level_name(max_level+1, displayname)

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


    def curve_number_to_span_number_at_level(self, curve_number):
        # flatten all currently visible spans's span number
        # `curve_number` is the index of the item that has been plotted
        # in a flattened list of all the curves
        span_numbers = []
        for i in range(self.get_current_max_level()+1):
            spanlists = self.levels[i]['spanlists']
            print("Spanlists: {0}".format(spanlists))
            span_numbers += list(range(len(spanlists)))
        print(span_numbers)
        print("Curve Number: {0}, Span Number (at some level): {1}".format(curve_number, span_numbers[curve_number]))
        return span_numbers[curve_number]


    def expand_span_at_level_name(self, level_name, span_number):
        level_number = self.level_names.index(level_name)
        self.expand_span_at_level(level_number, span_number)

    def set_level_name(self, index, name):
        if index < len(self.level_names):
            self.level_names[index] = name
        elif index == len(self.level_names):
            self.level_names.append(name)
        else:
            raise Exception("Unexpected behavior, setting level name more than 1 level head of existing")


    def expand_span_at_level(self, level_number, span_number):

        if level_number not in self.levels:
            raise Exception("Cannot expand a level that isn't currently computed")

        level = self.levels[level_number]
        if level_number+1 in self.levels:
            next_level = self.levels[level_number+1]
            if span_number == next_level['descendsfrom']:
                return

        # delete all further levels down
        for i in range(level_number+1, self.get_current_max_level()+1):
            del self.levels[i]
            del self.level_names[i]

        # recompute level_number + 1
        spanlist = level['spanlists'][span_number]
        children = spanlist.get_child_spanlists()
        level_name = "Level {0}:<br> children of <br> {1}".format(level_number+1, spanlist.get_spans_name())
        self.levels[level_number + 1] = {
            'spanlists': children,
            'data': self._spanlists_to_plot_definition(level_name, children, style=self.style),
            'descendsfrom': span_number
        }
        self.set_level_name(level_number+1, level_name)

        # expand further if there's only 1 span
        self._expand_single_spans()
        return True

        # new figure is retrieved via get_figure()


    def get_figure(self):
        layout = go.Layout(
            title=self.graph_name,
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
            **self.layout_options
        )

        figure = {
            'data': self.get_plot_data(),
            'layout': layout
        }

        return figure

    def _spanlists_to_plot_definition(self, category, spanlists, style='box', x_axis_divisor=1000.0):
        """ SpanList[] to definitions of box or bar plot """
        data = []
        for spanlist in spanlists:
            x_data = spanlist.get_values_np()
            data.append({
                "x" : x_data/x_axis_divisor,
                "y" : [category] * x_data.shape[0],
                "name" : spanlist.get_spans_name(),
                "boxmean": True,
                "orientation": 'h',
                "type": style
            })
        return data
