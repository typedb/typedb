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
sorted_executions = get_sorted_executions(es_utility)


def get_sidebar(sorted_executions):
    existing_executions_radio = dcc.RadioItems(
        id="existing-executions-radio",
        options=[{'label': x, 'value': x} for x in sorted_executions],
        value=sorted_executions[0] # initialize with most recent one in sorted list
    )
    return html.Div(
        children=[
            html.H3("Benchmarks"),
            existing_executions_radio
        ],
        className="col-xl-1"
    )


def get_benchmark_column(benchmark, width="11"):
    return html.Div(className="col-xl-"+width, children=[
        benchmark
    ])



print("Generating layout...")
active_benchmark = html.Div(id='active-benchmark')
layout = html.Div(children=[
        html.H1("Grakn Benchmarking Dashboard"),
        html.Div(
            className="container-fluid",
            children=[
                html.Div(
                    className="row",
                    children=[
                        get_sidebar(sorted_executions),
                        get_benchmark_column(active_benchmark),
                        html.Div(id='testing-output', children=['TESTING'])
                    ]
                )
            ]
        )
    ])

# bootstrap CSS
# app.css.append_css({
#     'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
# })
app.css.append_css({
    'external_url': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css'
})
app.layout = layout


# ---- interactivity functionality ----

executions = {}

def try_create_execution(execution_name):
    if execution_name not in executions:
        execution_number = sorted_executions.index(execution_name)
        executions[execution_name] = BenchmarkExecutionComponent(app, es_utility, execution_name, execution_number)


@app.callback(
    dash.dependencies.Output('active-benchmark', 'children'),
    [dash.dependencies.Input('existing-executions-radio', 'value')])
def execution_updated(execution_name):
    try_create_execution(execution_name)
    return executions[execution_name].full_render()


for i, execution_name in enumerate(sorted_executions):
    # create a app.callback for each possible required callback in BenchmarkExecutionComponent
    callback_definitions = BenchmarkExecutionComponent.get_required_callback_definitions(i)

    def route_execution_callback(method_name):
        print("creating callback router for {0}".format(method_name))

        def wrapped_callback(*args):
            try_create_execution(execution_name)
            execution = executions[execution_name]
            return execution.route_callback(method_name, *args)
        return wrapped_callback

    for callback_function_name in callback_definitions:
        callback_definition = callback_definitions[callback_function_name]
        print("Assigning a callback with definitions {0}".format(callback_definition))
        app.callback(callback_definition[0], callback_definition[1])(route_execution_callback(callback_function_name))


if __name__ == '__main__':
    app.run_server(debug=True)
