from ..Concept.ConceptFactory import ConceptFactory
from . import RequestBuilder

class ResponseConverter(object):

    def __init__(self, tx_service):
        self._tx_service = tx_service 
        self._concept_factory = ConceptFactory(tx_service)
        self._answer_converter = AnswerConverter(self._concept_factory)


    def query(self, grpc_query_iter):
        iterator_id = grpc_query_iter.id
        return ResponseIterator(self._tx_service,
                                iterator_id,
                                lambda res: self._answer_converter.convert(res.iterate_res.query_iter_res.answer))


    def get_concept(self, grpc_get_schema_concept):
        which_one = grpc_get_schema_concept.WhichOneof("res")
        if which_one == "concept":
            grpc_concept = grpc_get_schema_concept.concept
            return self._concept_factory.create_concept(grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise Exception("Unknown getConcept response: {0}".format(which_one))


    def get_schema_concept(self, grpc_get_concept):
        which_one = grpc_get_concept.WhichOneof("res")
        if which_one == "schemaConcept":
            grpc_concept = grpc_get_concept.schemaConcept
            return self._concept_factory.create_concept(grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise Exception("Unknown getSchemaConcept response: {0}".format(which_one))

    def get_attributes_by_value(self, grpc_get_attrs_iter):
        iterator_id = grpc_get_attrs_iter.id
        return ResponseIterator(self._tx_service,
                                iterator_id,
                                lambda res: self._concept_factory.create_concept(res.iterate_res.getAttributes_iter_res.attribute))

    def put_entity_type(self, grpc_put_entity):
        

        
         
         



class Explanation(object):
    def __init__(self, query_pattern, answers):
        self._query_pattern = query_pattern
        self._answers = answers
    def query_pattern(self):
        return self._query_pattern
    def answers(self):
        return self._answers

class Answer(object):
    def __init__(self, answer_map, explanation: Explanation):
        self._answer_map = answer_map
        self._explanation = explanation
    def get(self): # optional argument: variable to get
        # TODO this needs to return a Concept to match Java Answer API
        return self._answer_map
    def explanation(self):
        return self._explanation


class AnswerConverter(object):

    def __init__(self, concept_factory):
        self._concept_factory = concept_factory

    def convert(self, grpc_answer):
        which_one = grpc_answer.WhichOneof('answer')

        if which_one == 'queryAnswer':
           return self._create_query_answer(grpc_answer.queryAnswer) 
        elif which_one == 'computeAnswer':
            return self._create_compute_answer(grpc_answer.computeAnswer)
        elif which_one == 'otherResult':
            return self._create_other_answer(grpc_answer.otherResult)
        else:
            # TODO refine exception
            raise Exception('Unknown Answer.answer message type: {0}'.format(which_one))

    def _create_query_answer(self, grpc_query_answer):
        """ Create grpc QueryAnswer message into object """
        var_concept_map = grpc_query_answer.queryAnswer
        # build the answer concepts
        answer_map = {}
        for (variable, grpc_concept) in var_concept_map.items():
            answer_map[variable] = self._concept_factory.create_concept(grpc_concept)

        # build the explanation
        explanation = self._create_explanation(grpc_query_answer.explanation)
        return Answer(answer_map, explanation)

    def _create_explanation(self, grpc_explanation):
        """ Convert grpc Explanation message into object """
        query_pattern = grpc_explanation.queryPattern
        grpc_answers = grpc_explanation.queryAnswer
        answers = [] 
        for grpc_ans in grpc_answers:
            answers.append(self._create_query_answer(grpc_ans))
        return Explanation(query_pattern, answers)


    def _create_compute_answer(self, grpc_compute_answer):
        # TODO
        pass


    def _create_other_result(self, grpc_other_result):
        # TODO
        pass



class ResponseIterator(object):

    def __init__(self, tx_service , iterator_id, iter_resp_converter):
        self._tx_service = tx_service  
        self.iterator_id = iterator_id
        self._iter_resp_converter = iter_resp_converter

    def __iter__(self):
        return self

    def __next__(self):
        # get next from server
        iter_response = self._tx_service.iterate(self.iterator_id)
        print("Iterator response:")
        print(iter_response)
        which_one = iter_response.WhichOneof("res")
        if which_one == 'done' and iter_response.done:
            raise StopIteration()
        else:
            print(iter_response)
            return self._iter_resp_converter(iter_response)





