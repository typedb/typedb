import dash
import dash_html_components as html
import dash_core_components as dcc
import datetime
import argparse

from BenchmarkExecutionComponent import BenchmarkExecutionComponent
import elasticsearch_helper as es_helper



parser = argparse.ArgumentParser(description="Run Grakn Benchmarking dashboard to visualize data generated \
                                             and stored in ElasticSearch")
parser.add_argument('--max-graphs', dest="max_graphs", default=100, help="Number of graphs that can be interacted with \
                                                                         for i any benchmark execution (default 100)")


def get_sorted_executions(es):
    """ Obtain from elasticsearch the existing benchmarking executions and return sorted by date """
    existing_executions = es.get_all_execution_names()
    # split and sort by formatted date
    date_format = "%Y-%m-%d %H:%M"
    parser = lambda date_string: datetime.datetime.strptime(date_string, date_format)
    pairs = [(parser(x[:x.find(':')+2]), x) for x in existing_executions]
    pairs.sort(reverse=True, key=lambda pair: pair[0])
    return [x[1] for x in pairs]


# --- layout ---- 

def get_executions_sidebar_layout(sorted_executions):
    """ Generate HTML for the list of benchmark executions as a column """
    executions_radio = dcc.RadioItems(
        id="existing-executions-radio",
        options=[{'label': x, 'value': x} for x in sorted_executions],
        value=sorted_executions[0] # initialize with most recent one in sorted list
    )
    return html.Div(
        children=[
            html.H3("Benchmarks"),
            executions_radio
        ],
        className="col-xl-1"
    )

def get_dashboard_layout(sorted_executions, benchmark_width=11):
    print("Generating layout...")
    
    # placeholder for benchmark graphs with a specified width
    active_benchmark = html.Div(
            classname="col-xl-{0}".format(benchmark_width), 
            children=[
                html.Div(id='active-benchmark')
            ]
        )
    
    # sidebar with set width and radio buttons with executions
    sidebar = get_executions_sidebar_layout(sorted_executions)

    layout = html.Div(children=[
            html.H1("Grakn Benchmarking Dashboard"),
            html.Div(
                className="container-fluid",
                children=[
                    html.Div(
                        className="row",
                        children=[
                            sidebar,
                            active_benchmark
                        ]
                    )
                ]
            )
        ])

    return layout

# ---- interactivity functionality ----

def try_create_execution(execution_components, sorted_executions, execution_name):
    """ Create an BenchmarkExecutionComponent if it doesn't already exist in the given dictionary """
    if execution_name not in execution_components:
        execution_number = sorted_executions.index(execution_name)
        execution_components [execution_name] = BenchmarkExecutionComponent(app, es_utility, execution_name, execution_number)

# -- dynamic callbacks --


def attach_dynamic_callbacks(app, sexecution_component, sorted_executions):
    """ pre-compute the controls we will need to generate graphs, all callbacks must be declared before server starts """
    for i, execution_name in enumerate(sorted_executions):
        # create a app.callback for each possible required callback in BenchmarkExecutionComponent
    
        def route_execution_callback(method_name, exec_name):
            # NOTE pass exec_name through to retain a copy from the loop, else python closures refer to last loop iter
            print("creating callback router for {1}.{0}.method_name".format(method_name, exec_name))
    
            def wrapped_callback(*args):
                # copy execution name using a lambda
                print("Callback with args, method aname and execution name: {0}, {1}, {2}".format(args, method_name, exec_name))
                try_create_execution(execution_components, sorted_executions, exec_name)
                execution = execution_components[exec_name]
                return execution.route_callback(method_name, *args)
            return wrapped_callback
    
        callback_definitions = BenchmarkExecutionComponent.get_required_callback_definitions(unique_number=i,
                                                                                             graph_callbacks=max_graphs)
    
        for callback_function_name in callback_definitions:
            callback_definition = callback_definitions[callback_function_name]
            app.callback(callback_definition[0], callback_definition[1])(route_execution_callback(callback_function_name, execution_name))

if __name__ == '__main__':
    args = parser.parse_args()
    max_graphs = args.max_graphs
    
    app = dash.Dash()

    # supress callback exceptions for nonexistant labels
    # since we're dynamically adding callbacks that don't have inputs/outputs yet
    app.config.supress_callback_exceptions = True

    # initialize ES utility
    es_utility = es_helper.ElasticsearchUtility()
    
    print("Retrieving existing benchmarks...")
    sorted_executions = get_sorted_executions(es_utility)
    
    # bootstrap CSS
    app.css.append_css({
        'external_url': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css'
    })
    app.layout = get_dashboard_layout(sorted_executions)
    
    
    execution_components = {}
   
    # ---- static calbacks, always required ----
    @app.callback(
        dash.dependencies.Output('active-benchmark', 'children'),
        [dash.dependencies.Input('existing-executions-radio', 'value')])
    def execution_updated(execution_name):
        try_create_execution(execution_components, sorted_executions, execution_name)
        return execution_components[execution_name].full_render()
    
  
    # --- dynamic callbacks ---
    attach_dynamic_callbacks(app, execution_components, sorted_executions)

    app.run_server(debug=True)
