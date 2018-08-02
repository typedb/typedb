import abc
from grakn.service.Session.util import enums
from grakn.service.Session.Concept import ConceptFactory


# TODO this file is a bit of a mess -- clean up

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
        if whichone == 'string':
            return grpc_value_object.string
        elif whichone == 'boolean':
            return grpc_value_object.boolean
        elif whichone == 'integer':
            return grpc_value_object.integer
        elif whichone == 'long':
            return grpc_value_object.long
        elif whichone == 'float':
            return grpc_value_object.float
        elif whichone == 'double':
            return grpc_value_object.double
        elif whichone == 'date':
            return grpc_value_object.date
        else:
            raise Exception("Unknown datatype in enum but not handled in from_grpc_value_object")
            



        

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
    def __init__(self, query_pattern, list_of_concept_maps):
        self._query_pattern = query_pattern
        self._concept_maps_list = list_of_concept_maps
        
    def query_pattern(self):
        return self._query_pattern

    def get_answers(self):
        """ Return answers this explanation is dependent on"""
        # note that concept_maps are subtypes of Answer
        return self._concept_maps_list


# ----- Different types of answers -----

class Answer(object):
    """ Top level answer, provides interface """

    def __init__(self, explanation: Explanation):
        self._explanation = explanation

    @abc.abstractmethod
    def get(self): 
        pass

    def explanation(self):
        return self._explanation

    def explanations(self):
        if self._explanation is None:
            return None
        
        explanations = set()
        for concept_map in self._explanation.get_answers():
            recursive_explanation_set = concept_map.explanations()
            explanations = explanations.union(recursive_explanation_set)
        return explanations


class ConceptMap(Answer):

    def __init__(self, concept_map, explanations):
        super().__init__(explanations)
        self._concept_map = concept_map 

    def get(self, var=None):
        """ Get the indicated variable's Concept from the map or this ConceptMap """
        if var is None:
            return self
        else:
            if var not in self._concept_map:
                # TODO specialize exception
                raise Exception("{0} is not in the ConceptMap".format(var))
            return self._concept_map[var]
            """ Return ConceptMap """
            return self
    
    def map(self):
        """ Get the map from Variable (str) to Concept objects """
        return self._concept_map

    def vars(self):
        """ Get a set of vars in the map """
        return set(self._concept_map.keys())

    def contains_var(self, var):
        """ Check whether the map contains the var """
        return var in self._concept_map

    def is_empty(self):
        """ Check if the variable map is empty """
        return len(self._concept_map) == 0

class ConceptList(Answer):

    def __init__(self, concept_id_list, explanation: Explanation):
        super().__init__(explanation)
        self._concept_id_list = concept_id_list

    def get(self):
        """ Get this ConceptList """
        return self._concept_id_list

    def list(self):
        """ Get the list of concept IDs """
        return self._concept_id_list

class ConceptSet(Answer):

    def __init__(self, concept_id_set, explanation: Explanation):
        super().__init__(explanation)
        self._concept_id_set = concept_id_set

    def get(self):
        """ Get this ConceptSet """
        return self

    def set(self):
        """ Return the set of Concept IDs within this ConceptSet """
        return self._concept_id_set

class Value(Answer):

    def __init__(self, number, explanation: Explanation):
        super().__init__(explanation)
        self._number = number

    def get(self):
        """ Get this Value object """
        return self

    def number(self):
        """ Get as number (float or int) """
        try:
            return int(self._number)
        except ValueError:
            return float(self._number)


class AnswerConverter(object):
    """ Static methods to convert answers into Answer objects """

    @staticmethod
    def convert(tx_service, grpc_answer):
        which_one = grpc_answer.WhichOneof('answer')

        if which_one == 'conceptMap':
           return AnswerConverter._create_concept_map(tx_service, grpc_answer.conceptMap) 
        elif which_one == 'conceptList':
            return AnswerConverter._create_concept_list(tx_service, grpc_answer.conceptList)
        elif which_one == 'conceptSet':
            return AnswerConverter._create_concept_set(tx_service, grpc_answer.conceptSet)
        elif which_one == 'conceptSetMeasure':
            return AnswerConverter._create_concept_set_measure(tx_service, grpc_answer.conceptSetMeasure)
        elif which_one == 'value':
            return AnswerConverter._create_value(tx_service, grpc_answer.value)
        else:
            # TODO refine exception
            raise Exception('Unknown Answer.answer message type: {0}'.format(which_one))
   
    @staticmethod
    def _create_concept_map(tx_service, grpc_concept_map_msg):
        """ Create a Concept Dictionary from the grpc response """
        var_concept_map = grpc_concept_map_msg.map
        answer_map = {}
        for (variable, grpc_concept) in var_concept_map.items():
            answer_map[variable] = ConceptFactory.create_concept(tx_service, grpc_concept)

        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_map_msg.explanation)
        return ConceptMap(answer_map, explanation)

    @staticmethod
    def _create_concept_list(tx_service, grpc_concept_list_msg):
        ids_list = list(grpc_concept_list_msg.list.ids)
        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_list_msg.explanation)
        return ConceptList(ids_list, explanation)

    @staticmethod
    def _create_concept_set(tx_service, grpc_concept_set_msg):
        ids_set = set(grpc_concept_set_msg.list.ids)
        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_set_msg.explanation)
        return ConceptSet(ids_set, explanation)

    @staticmethod
    def _create_concept_set_measure(tx_service, grpc_concept_set_measure):
        concept_ids = list(grpc_concept_set_measure.set.ids)
        number = grpc_concept_set_measure.measurement.value # TODO cast string to number
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_set_measure.explanation)

    @staticmethod
    def _create_value(tx_service, grpc_value_msg):
        number = grpc_value_msg.number.value # TODO cast string to number
        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_value_msg.explanation)
        return Value(number, explanation)

    @staticmethod
    def _create_explanation(tx_service, grpc_explanation):
        """ Convert grpc Explanation message into object """
        query_pattern = grpc_explanation.pattern
        grpc_list_of_concept_maps = grpc_explanation.answers
        native_list_of_concept_maps = []
        for grpc_concept_map in grpc_list_of_concept_maps:
            native_list_of_concept_maps.append(AnswerConverter._create_concept_map(tx_service, grpc_concept_map))
        return Explanation(query_pattern, native_list_of_concept_maps)



class ResponseIterator(object):
    """ Retrieves next value in the sequence """

    def __init__(self, tx_service , iterator_id, iter_resp_converter):
        self._tx_service = tx_service  
        self.iterator_id = iterator_id
        self._iter_resp_converter = iter_resp_converter

    def __iter__(self):
        return self

    def __next__(self):
        # get next from server
        iter_response = self._tx_service.iterate(self.iterator_id)
        # print("Iterator response:")
        # print(iter_response)
        which_one = iter_response.WhichOneof("res")
        if which_one == 'done' and iter_response.done:
            raise StopIteration()
        else:
            return self._iter_resp_converter(self._tx_service, iter_response)

    def collect_concepts(self):
        """ Helper method to retrieve concepts from a query() method """
        concepts = []
        for answer in self:
            if type(answer) != ConceptMap:
                # TODO specialize exception
                raise Exception("Only use .collect_concepts on ConceptMaps returned by query()")
            concepts.extend(answer.map().values()) # get concept map => concepts
        return concepts





