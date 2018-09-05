import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
import elasticsearch_helper as es_helper
import datetime
import numpy as np
import pandas as pd
import time

from BenchmarkExecutionComponent import BenchmarkExecutionComponent


app = dash.Dash()
# supress callback exceptions for nonexistant labels
# since we're dynamically adding callbacks
app.config.supress_callback_exceptions = True
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

print("Retrieving existing benchmarks...")

# obtain the existing executions and turn them into a radio button
sorted_executions = get_sorted_executions(es_utility)
existing_executions_radio = dcc.RadioItems(
        id="existing-executions-radio",
        options=[{'label': x, 'value': x} for x in sorted_executions], 
        value=sorted_executions[0] # initialize with most recent one in sorted list
    )

sidebar = html.Div(children=[
    html.H3("Benchmarks"),
    existing_executions_radio
    ], className="sidebar")
active_benchmark = html.Div(id='active-benchmark')
layout = html.Div(children=[
        html.H1("Grakn Benchmarking Dashboard"),
        html.Div(id="hidden stuff",
            style={'display': 'none'},
            children=[
                 dcc.Graph(id="adsfasdf"),
                 dcc.Dropdown(id="asdfasdfasdf")
             ],
        ),
        sidebar,
        active_benchmark,
        html.Div(id='test-output')
    ])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})
app.layout = layout



# ---- interactivity functionality ----

executions = {}
@app.callback(
    dash.dependencies.Output('active-benchmark', 'children'),
    [dash.dependencies.Input('existing-executions-radio', 'value')])
def execution_updated(execution_name):
    if execution_name not in executions:
        executions[execution_name] = BenchmarkExecutionComponent(app, es_utility, execution_name)
    print(app.callback_map)
    return executions[execution_name].full_render()
    # return dcc.RadioItems(
    #     id='test-output',
    #     options = [{'label': x, 'value': x} for x in sorted_executions],
    #     value=sorted_executions[-1]
    # )

@app.callback(
    dash.dependencies.Output('test-output', 'children'),
    [dash.dependencies.Input('test-input', 'value')])
def test(value):
    return value





if __name__ == '__main__':
    app.run_server(debug=True)
