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

app.layout = html.Div(children=[
        html.H1("Grakn Benchmarking Dashboard"),
        sidebar
    ])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})


# ---- interactivity functionality ----

executions = {}
@app.callback(
    dash.dependencies.Output('active_benchmark', 'children'),
    [dash.dependencies.Input('existing-executions-radio', 'value')])
def execution_updated(execution_name):
    if execution_name not in executions:
        executions[execution_name] = BenchmarkExecutionComponent(app, es_utility, execution_name)
    return executions[execution_name].full_render()





if __name__ == '__main__':
    app.run_server(debug=True)
