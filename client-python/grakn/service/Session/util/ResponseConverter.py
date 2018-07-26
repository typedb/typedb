from grakn.service.Session.util import enums
from grakn.service.Session.Concept import ConceptFactory

class ResponseConverter(object):
    
    @staticmethod
    def query(tx_service, grpc_query_iter):
        iterator_id = grpc_query_iter.id
        return ResponseIterator(tx_service,
                                iterator_id,
                                lambda tx_serv, iterate_res: AnswerConverter.convert(tx_serv, iterate_res.query_iter_res.answer))


    @staticmethod
    def get_concept(tx_service, grpc_get_schema_concept):
        which_one = grpc_get_schema_concept.WhichOneof("res")
        if which_one == "concept":
            grpc_concept = grpc_get_schema_concept.concept
            return ConceptFactory.create_concept(tx_service, grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise Exception("Unknown getConcept response: {0}".format(which_one))

    @staticmethod
    def get_schema_concept(tx_service, grpc_get_concept):
        which_one = grpc_get_concept.WhichOneof("res")
        if which_one == "schemaConcept":
            grpc_concept = grpc_get_concept.schemaConcept
            return ConceptFactory.create_concept(tx_service, grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise Exception("Unknown getSchemaConcept response: {0}".format(which_one))

    @staticmethod
    def get_attributes_by_value(tx_service, grpc_get_attrs_iter):
        iterator_id = grpc_get_attrs_iter.id
        return ResponseIterator(tx_service,
                                iterator_id,
                                lambda tx_serv, iterate_res: ConceptFactory.create_concept(tx_serv, iterate_res.getAttributes_iter_res.attribute))

    @staticmethod
    def put_entity_type(tx_service, grpc_put_entity_type):
        return ConceptFactory.create_concept(tx_service, grpc_put_entity_type.entityType) 

    @staticmethod
    def put_relationship_type(tx_service, grpc_put_relationship_type):
        return ConceptFactory.create_concept(tx_service, grpc_put_relationship_type.relationType)

    @staticmethod
    def put_attribute_type(tx_service, grpc_put_attribute_type):
        return ConceptFactory.create_concept(tx_service, grpc_put_attribute_type.attributeType)

    @staticmethod
    def put_role(tx_service, grpc_put_role):
        return ConceptFactory.create_concept(tx_service, grpc_put_role.role)

    @staticmethod
    def put_rule(tx_service, grpc_put_rule):
        return ConceptFactory.create_concept(tx_service, grpc_put_rule.rule)

    @staticmethod
    def from_grpc_value_object(grpc_value_object):
        whichone = grpc_value_object.WhichOneof('value')
        # check the one is in the known datatypes
        known_datatypes = [e.name.lower() for e in enums.DataType]
        if whichone.lower() not in known_datatypes:
            raise Exception("Unknown value object value key: {0}, not in {1}".format(whichone, known_datatypes))
        return whichone
            



        

    # --- concept method helpers ---

    @staticmethod
    # TODO refactor all iterators to use this directly, much more compact & still easy to read
    def iter_res_to_iterator(tx_service, iterator_id, next_iteration_handler):
        return ResponseIterator(tx_service, iterator_id, next_iteration_handler)


    class SchemaConcept(object):
        @staticmethod
        def subs_iterator(tx_service, grpc_subs_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_subs_iter.id,
                    lambda tx_serv, iter_res: 
                        ConceptFactory.create_concept(tx_serv,  
                        iter_res.conceptMethod_iter_res.schemaConcept_subs_iter_res.schemaConcept)
                    )
    
        @staticmethod
        def sups_iterator(tx_service, grpc_sups_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_sups_iter.id,
                    lambda tx_serv, iter_res:
                        ConceptFactory.create_concept(tx_serv, 
                        iter_res.conceptMethod_iter_res.schemaConcept_sups_iter_res.schemaConcept)
                    )
    
    class Type(object):                                        
        @staticmethod
        def attributes(tx_service, grpc_type_attributes_iter): 
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_type_attributes_iter.id,
                    lambda tx_serv, iter_res: 
                        ConceptFactory.create_concept(tx_serv,
                        iter_res.conceptMethod_iter_res.type_attributes_iter_res.attributeType)
                    )
    
        @staticmethod
        def instances(tx_service, grpc_type_instances_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_type_instances_iter.id,
                    lambda tx_serv, iter_res: 
                        ConceptFactory.create_concept(tx_serv,
                        iter_res.conceptMethod_iter_res.type_instances_iter_res.thing)
                    )
        
        @staticmethod
        def playing(tx_service, grpc_playing_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_playing_iter.id,
                    lambda tx_serv, iter_res:
                        ConceptFactory.create_concept(tx_serv,
                        iter_res.conceptMethod_iter_res.type_playing_iter_res.role)
                    )

        @staticmethod
        def keys(tx_service, grpc_keys_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_keys_iter.id,
                    lambda tx_serv, iter_res:
                        ConceptFactory.create_concept(tx_serv,
                        iter_res.conceptMethod_iter_res.type_keys_iter_res.attributeType)
                    )
                                        
    class RelationshipType(object):
                                            
        @staticmethod
        def roles(tx_service, grpc_roles_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_roles_iter.id,
                    lambda tx_serv, iter_res:
                        ConceptFactory.create_concept(tx_serv,
                        iter_res.conceptMethod_iter_res.relationType_roles_iter_res.role)
                    )

    class Role(object):

        @staticmethod
        def relations(tx_service, grpc_relations_iter):
            return ResponseConverter.iter_res_to_iterator(
                    tx_service,
                    grpc_relations_iter.id,
                    lambda tx_serv, iter_res:
                        ConceptFactory.create_concept(tx_serv,
                        iter_res.conceptMethod_iter_res.role_relations_iter_res.relationType))

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

    def get(self, var=None): # optional argument: variable to get
        if var is None:
            return self._answer_map
        else:
            try:
                return self._answer_map[var]
            except Exception as e:
                raise e

    def explanation(self):
        return self._explanation


class AnswerConverter(object):
    """ Static methods to convert answers into Answer objects """

    @staticmethod
    def convert(tx_service, grpc_answer):
        which_one = grpc_answer.WhichOneof('answer')

        if which_one == 'queryAnswer':
           return AnswerConverter._create_query_answer(tx_service, grpc_answer.queryAnswer) 
        elif which_one == 'computeAnswer':
            return AnswerConverter._create_compute_answer(grpc_answer.computeAnswer)
        elif which_one == 'otherResult':
            return AnswerConverter._create_other_result(grpc_answer.otherResult)
        else:
            # TODO refine exception
            raise Exception('Unknown Answer.answer message type: {0}'.format(which_one))
    
    @staticmethod
    def _create_query_answer(tx_service, grpc_query_answer):
        """ Create grpc QueryAnswer message into object """
        var_concept_map = grpc_query_answer.queryAnswer
        # build the answer concepts
        answer_map = {}
        for (variable, grpc_concept) in var_concept_map.items():
            answer_map[variable] = ConceptFactory.create_concept(tx_service, grpc_concept)

        # build the explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_query_answer.explanation)
        return Answer(answer_map, explanation)

    @staticmethod
    def _create_explanation(tx_service, grpc_explanation):
        """ Convert grpc Explanation message into object """
        query_pattern = grpc_explanation.queryPattern
        grpc_answers = grpc_explanation.queryAnswer
        answers = [] 
        for grpc_ans in grpc_answers:
            answers.append(AnswerConverter._create_query_answer(tx_service, grpc_ans))
        return Explanation(query_pattern, answers)

    @staticmethod
    def _create_compute_answer(grpc_compute_answer):
        # TODO
        pass

    @staticmethod
    def _create_other_result(grpc_other_result):
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
            return self._iter_resp_converter(self._tx_service, iter_response)





