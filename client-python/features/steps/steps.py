from behave import *
from behave.runner import Context
from nose.tools import eq_

import features.environment as env


use_step_matcher("re")


@given("a knowledge base")
def step_impl(context: Context):
    context.open_client()


@given("schema `(.*)`")
def step_impl(context: Context, patterns: str):
    env.define(patterns)


@given("data `(.*)`")
def step_impl(context: Context, patterns: str):
    env.insert(patterns)


@given("a broken connection to the database")
def step_impl(context: Context):
    context.open_client(env.broken_connection)


@given("inference is disabled")
def step_impl(context: Context):
    context.params = { 'infer': False }


@when('the user issues `(.*)`')
def step_impl(context: Context, query: str):
    context.execute_query(query)


@then("the response is `(.*)`")
def step_impl(context: Context, response: str):
    eq_(context.get_response(), eval(response))


# TODO: Re-think if these steps are really the same
@then("return a response with (new|existing) concepts")
def step_impl(context: Context, concept_kind: str):
    assert len(context.get_response()) > 0, f"Response was empty: {context.get_response()}"


@then("the response has (\d+|no) results?")
def step_impl(context, num_results: str):
    if num_results == "no":
        num_results = "0"
    num_results = int(num_results)
    eq_(len(context.get_response()), num_results)


@then("the response is empty")
def step_impl(context: Context):
    eq_(context.get_response(), None)


@then('the type "(.*)" is in the knowledge base')
def step_impl(context: Context, label: str):
    assert env.check_type(label)


@then('the instance with (.*) "(.*)" is in the knowledge base')
def step_impl(context: Context, resource_label: str, value: str):
    assert env.check_instance(resource_label, value)


@then("return an error")
def step_impl(context: Context):
    assert context.get_error() is not None
