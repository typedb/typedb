#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import abc
import datetime
from grakn.service.Session.util import enums
from grakn.service.Session.Concept import ConceptFactory
from grakn.exception.GraknError import GraknError 


class ResponseReader(object):
    
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
            raise GraknError("Unknown get_concept response: {0}".format(which_one))

    @staticmethod
    def get_schema_concept(tx_service, grpc_get_concept):
        which_one = grpc_get_concept.WhichOneof("res")
        if which_one == "schemaConcept":
            grpc_concept = grpc_get_concept.schemaConcept
            return ConceptFactory.create_concept(tx_service, grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise GraknError("Unknown get_schema_concept response: {0}".format(which_one))

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
            raise GraknError("Unknown value object value key: {0}, not in {1}".format(whichone, known_datatypes))
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
            epoch_ms_utc = grpc_value_object.date
            local_datetime_utc = datetime.datetime.fromtimestamp(float(epoch_ms_utc)/1000.)
            return local_datetime_utc
        else:
            raise GraknError("Unknown datatype in enum but not handled in from_grpc_value_object")
        

    # --- concept method helpers ---

    @staticmethod
    def iter_res_to_iterator(tx_service, iterator_id, next_iteration_handler):
        return ResponseIterator(tx_service, iterator_id, next_iteration_handler)

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

class AnswerGroup(Answer):

    def __init__(self, owner_concept, answer_list, explanation):
        super().__init__(explanation)
        self._owner_concept = owner_concept
        self._answer_list = answer_list

    def get(self):
        return self

    def owner(self):
        return self._owner_concept

    def answers(self):
        return self._answer_list



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
                raise GraknError("Variable {0} is not in the ConceptMap".format(var))
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

class ConceptSetMeasure(ConceptSet):

    def __init__(self, concept_id_set, number, explanation: Explanation):
        super().__init__(concept_id_set, explanation)
        self._measurement = number

    def measurement(self):
        return self._measurement


class Value(Answer):

    def __init__(self, number, explanation: Explanation):
        super().__init__(explanation)
        self._number = number

    def get(self):
        """ Get this Value object """
        return self

    def number(self):
        """ Get as number (float or int) """
        return self._number


class AnswerConverter(object):
    """ Static methods to convert answers into Answer objects """

    @staticmethod
    def convert(tx_service, grpc_answer):
        which_one = grpc_answer.WhichOneof('answer')

        if which_one == 'conceptMap':
           return AnswerConverter._create_concept_map(tx_service, grpc_answer.conceptMap) 
        elif which_one == 'answerGroup':
            return AnswerConverter._create_answer_group(tx_service, grpc_answer.answerGroup)
        elif which_one == 'conceptList':
            return AnswerConverter._create_concept_list(tx_service, grpc_answer.conceptList)
        elif which_one == 'conceptSet':
            return AnswerConverter._create_concept_set(tx_service, grpc_answer.conceptSet)
        elif which_one == 'conceptSetMeasure':
            return AnswerConverter._create_concept_set_measure(tx_service, grpc_answer.conceptSetMeasure)
        elif which_one == 'value':
            return AnswerConverter._create_value(tx_service, grpc_answer.value)
        else:
            raise GraknError('Unknown gRPC Answer.answer message type: {0}'.format(which_one))
   
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
    def _create_answer_group(tx_service, grpc_answer_group):
        grpc_owner_concept = grpc_answer_group.owner
        owner_concept = ConceptFactory.create_concept(tx_service, grpc_owner_concept)
        grpc_answers = list(grpc_answer_group.answers)
        answer_list = [AnswerConverter.convert(tx_service, grpc_answer) for grpc_answer in grpc_answers]
        explanation = AnswerConverter._create_explanation(tx_service, grpc_answer_group.explanation)
        return AnswerGroup(owner_concept, answer_list, explanation)

    @staticmethod
    def _create_concept_list(tx_service, grpc_concept_list_msg):
        ids_list = list(grpc_concept_list_msg.list.ids)
        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_list_msg.explanation)
        return ConceptList(ids_list, explanation)

    @staticmethod
    def _create_concept_set(tx_service, grpc_concept_set_msg):
        ids_set = set(grpc_concept_set_msg.set.ids)
        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_set_msg.explanation)
        return ConceptSet(ids_set, explanation)

    @staticmethod
    def _create_concept_set_measure(tx_service, grpc_concept_set_measure):
        concept_ids = list(grpc_concept_set_measure.set.ids)
        number = grpc_concept_set_measure.measurement.value 
        explanation = AnswerConverter._create_explanation(tx_service, grpc_concept_set_measure.explanation)
        return ConceptSetMeasure(concept_ids, AnswerConverter._number_string_to_native(number), explanation)

    @staticmethod
    def _create_value(tx_service, grpc_value_msg):
        number = grpc_value_msg.number.value 
        # build explanation
        explanation = AnswerConverter._create_explanation(tx_service, grpc_value_msg.explanation)
        return Value(AnswerConverter._number_string_to_native(number), explanation)

    @staticmethod
    def _create_explanation(tx_service, grpc_explanation):
        """ Convert grpc Explanation message into object """
        query_pattern = grpc_explanation.pattern
        grpc_list_of_concept_maps = grpc_explanation.answers
        native_list_of_concept_maps = []
        for grpc_concept_map in grpc_list_of_concept_maps:
            native_list_of_concept_maps.append(AnswerConverter._create_concept_map(tx_service, grpc_concept_map))
        return Explanation(query_pattern, native_list_of_concept_maps)

    @staticmethod
    def _number_string_to_native(number):
        try:
            return int(number)
        except ValueError:
            return float(number)



class ResponseIterator(object):
    """ Retrieves next value in the Grakn response iterator """

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
                raise GraknError("Only use .collect_concepts on ConceptMaps returned by query()")
            concepts.extend(answer.map().values()) # get concept map => concepts
        return concepts





