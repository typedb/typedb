import json
from concurrent import futures
from typing import Callable, Optional, Iterator, List, Union, Any

import grpc

import concept_pb2
import grakn_pb2_grpc
from concept_pb2 import Concept, ConceptId, ConceptMethod, Unit, ConceptResponse, Label
from grakn_pb2 import TxResponse, Done, TxRequest, Answer, QueryResult, RunConceptMethod
from grakn_pb2_grpc import GraknServicer
from iterator_pb2 import IteratorId, Next

DONE = TxResponse(done=Done())

error_type = 'GRAQL_SYNTAX_EXCEPTION'

ITERATOR_ID = IteratorId(id=5)

ITERATOR_RESPONSE = TxResponse(iteratorId=ITERATOR_ID)

NEXT = TxRequest(next=Next(iteratorId=ITERATOR_ID))

query: str = 'match $x sub concept; limit 3;'

grpc_answers = [
    Answer(answer={'x': Concept(id=ConceptId(value='a'), baseType=concept_pb2.MetaType)}),
    Answer(answer={'x': Concept(id=ConceptId(value='b'), baseType=concept_pb2.Attribute)}),
    Answer(answer={'x': Concept(id=ConceptId(value='c'), baseType=concept_pb2.AttributeType)})
]

grpc_responses = [QueryResult(answer=grpc_answer) for grpc_answer in grpc_answers]

error_message: str = 'sorry we changed the syntax again'


def eq(tx_request: TxRequest):
    return lambda other: other == tx_request


class MockResponse:
    """
    A mocked response to a matching request over gRPC.

    >>> tx_response = TxResponse(done=Done())
    >>> mock_response = MockResponse(lambda req: req.execQuery.query.value == "match $x isa person; get;", tx_response)
    """

    def __init__(self, request_matcher: Callable[[TxRequest], bool], response: TxResponse = None, error: str = None):
        assert response is None or error is None
        self._request_matcher = request_matcher
        self._response = response
        self._error = error

    def test(self, request: TxRequest) -> Optional[TxResponse]:
        if self._request_matcher(request):
            if self._response is not None:
                return self._response
            else:
                return self._error
        else:
            return None


class MockGraknServicer(GraknServicer):
    """A mock implementation of GraknServicer that has a set of mocked responses that can be set."""

    def __init__(self):
        self._responses = []
        self._requests = None

    @property
    def requests(self) -> List[TxRequest]:
        return list(self._requests)

    def init(self, responses: List[TxResponse]):
        self._responses = list(responses)
        self._requests = None

    def Tx(self, request_iterator: Iterator[TxRequest], context: grpc.ServicerContext) -> Iterator[TxResponse]:
        self._requests = []

        for request in request_iterator:
            print(f"REQUEST: {request}")
            self._requests.append(request)

            for mock_response in self._responses:
                tx_response = mock_response.test(request)
                if tx_response is not None:
                    print(f"RESPONSE: {tx_response}")
                    self._responses.remove(mock_response)

                    if isinstance(tx_response, TxResponse):
                        yield tx_response
                    else:
                        # return an error message
                        context.set_trailing_metadata([("ErrorType", error_type)])
                        context.abort(grpc.StatusCode.UNKNOWN, tx_response)

                    break
            else:
                print(f"RESPONSE: {DONE}")
                yield DONE

    def Delete(self, request, context):
        raise NotImplementedError


class GrpcServer:
    """A gRPCs server containing a MockGraknServicer"""

    @property
    def requests(self) -> List[TxRequest]:
        return list(self._servicer.requests)

    def __init__(self):
        servicer = MockGraknServicer()

        thread_pool = futures.ThreadPoolExecutor()
        server = grpc.server(thread_pool)
        grakn_pb2_grpc.add_GraknServicer_to_server(servicer, server)
        server.add_insecure_port('[::]:48556')
        server.start()

        self._servicer = servicer
        self._server = server

    def init(self, responses: List[MockResponse]):
        self._servicer.init(responses)

    def stop(self):
        self._server.stop(False)


class MockEngine:
    def verify(self, predicate: Union[TxRequest, Callable[[TxRequest], bool]]):
        """Assert that a TxRequest has been sent matching the given predicate"""
        assert self._test_tx_request(predicate), f"Expected {predicate}"

    def _test_tx_request(self, predicate: Union[TxRequest, Callable[[TxRequest], bool]]) -> bool:
        if predicate in self._server.requests:
            return True
        else:
            try:
                if any(predicate(r) for r in self._server.requests):
                    return True
                else:
                    return False
            except TypeError:
                return False

    def __init__(self, responses: List[MockResponse]):
        self._server = GrpcServer()
        self._responses = responses

    def __enter__(self):
        self._server.init(self._responses)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._server.stop()


def _mock_label_response(cid: str, label: str) -> MockResponse:
    run_concept_method = RunConceptMethod(id=ConceptId(value=cid), conceptMethod=ConceptMethod(getLabel=Unit()))
    concept_response = ConceptResponse(label=Label(value=label))
    request = TxRequest(runConceptMethod=run_concept_method)
    return MockResponse(eq(request), TxResponse(conceptResponse=concept_response))


def _mock_value_response(cid: str, value: concept_pb2.AttributeValue) -> MockResponse:
    run_concept_method = RunConceptMethod(id=ConceptId(value=cid), conceptMethod=ConceptMethod(getValue=Unit()))
    concept_response = ConceptResponse(attributeValue=value)
    request = TxRequest(runConceptMethod=run_concept_method)
    return MockResponse(eq(request), TxResponse(conceptResponse=concept_response))


def engine_responding_to_streaming_query() -> MockEngine:
    # respond with an iterator to execQuery request
    mock_responses = [MockResponse(_is_exec_query, ITERATOR_RESPONSE)]

    # respond with a bunch of query results each time NEXT is called
    mock_responses += [MockResponse(eq(NEXT), TxResponse(queryResult=grpc_response)) for grpc_response in
                       grpc_responses]

    # the last time NEXT is called, return DONE
    mock_responses.append(MockResponse(eq(NEXT), DONE))

    # return the correct labels and other properties for each concept
    mock_responses += [
        _mock_label_response('a', 'concept'),
        _mock_value_response('b', concept_pb2.AttributeValue(long=100)),
        _mock_label_response('c', 'resource')
    ]

    return MockEngine(mock_responses)


def engine_responding_to_single_answer_query(answer: Any) -> MockEngine:
    mock_responses = [MockResponse(_is_exec_query, TxResponse(queryResult=QueryResult(otherResult=json.dumps(answer))))]
    return MockEngine(mock_responses)


def engine_responding_to_void_query() -> MockEngine:
    mock_responses = [MockResponse(_is_exec_query, DONE)]
    return MockEngine(mock_responses)


def engine_responding_with_nothing() -> MockEngine:
    return MockEngine([])


def engine_responding_bad_request() -> MockEngine:
    error_response = MockResponse(lambda req: req.HasField('execQuery'), error=error_message)
    return MockEngine([error_response])


def _is_exec_query(request: TxRequest) -> bool:
    return request.execQuery.query.value == query
