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
from datetime import datetime

import protocol.session.Session_pb2 as transaction_messages
import protocol.session.Concept_pb2 as concept_messages
from grakn.service.Session.util import enums
from grakn.service.Session.Concept import BaseTypeMapping

import six

class RequestBuilder(object):
    """ Static methods for generating GRPC requests """

    @staticmethod
    def concept_method_req_to_tx_req(concept_id, grpc_concept_method_req):
        concept_method_req = transaction_messages.Transaction.ConceptMethod.Req()
        concept_method_req.id = concept_id
        concept_method_req.method.CopyFrom(grpc_concept_method_req)

        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.conceptMethod_req.CopyFrom(concept_method_req)
        return transaction_req

    # --- Top level functionality ---
    @staticmethod
    def open_tx(keyspace, tx_type, credentials):
        open_request = transaction_messages.Transaction.Open.Req()
        open_request.keyspace = keyspace
        open_request.type = tx_type.value
        if credentials is not None:
            open_request.username = credentials['username']
            open_request.password = credentials['password']

        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.open_req.CopyFrom(open_request)
        return transaction_req

    @staticmethod
    def query(query, infer=True):
        query_message = transaction_messages.Transaction.Query.Req()
        query_message.query = query
        query_message.infer = transaction_messages.Transaction.Query.INFER.Value("TRUE") if infer else \
                              transaction_messages.Transaction.Query.INFER.Value("FALSE")
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.query_req.CopyFrom(query_message)
        return transaction_req

    @staticmethod
    def commit():
        commit_req = transaction_messages.Transaction.Commit.Req()
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.commit_req.CopyFrom(commit_req)
        return transaction_req


    @staticmethod
    def get_concept(concept_id):
        get_concept_req = transaction_messages.Transaction.GetConcept.Req()
        get_concept_req.id = concept_id
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.getConcept_req.CopyFrom(get_concept_req)
        return transaction_req

    @staticmethod
    def get_schema_concept(label):
        get_schema_concept_req = transaction_messages.Transaction.GetSchemaConcept.Req()
        get_schema_concept_req.label = label
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.getSchemaConcept_req.CopyFrom(get_schema_concept_req)
        return transaction_req

    def get_attributes_by_value(value, datatype):
        get_attrs_req = transaction_messages.Transaction.GetAttributes.Req()
        grpc_value_object = RequestBuilder.ConceptMethod.as_value_object(value, datatype)
        get_attrs_req.value.CopyFrom(grpc_value_object)
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.getAttributes_req.CopyFrom(get_attrs_req)
        return transaction_req
    get_attributes_by_value.__annotations__ = {'datatype': enums.DataType}
    get_attributes_by_value = staticmethod(get_attributes_by_value)


    @staticmethod
    def put_entity_type(label):
        put_entity_type_req = transaction_messages.Transaction.PutEntityType.Req()
        put_entity_type_req.label = label
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.putEntityType_req.CopyFrom(put_entity_type_req)
        return transaction_req

    @staticmethod
    def put_relationship_type(label):
        # NOTE: Relation vs relationship
        put_relation_type_req = transaction_messages.Transaction.PutRelationType.Req()
        put_relation_type_req.label = label
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.putRelationType_req.CopyFrom(put_relation_type_req)
        return transaction_req

    def put_attribute_type(label, data_type):
        put_attribute_type_req = transaction_messages.Transaction.PutAttributeType.Req()
        put_attribute_type_req.label = label
        put_attribute_type_req.dataType = data_type.value # retrieve enum value
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.putAttributeType_req.CopyFrom(put_attribute_type_req)
        return transaction_req
    put_attribute_type.__annotations__ = {'data_type': enums.DataType}
    put_attribute_type = staticmethod(put_attribute_type)

    @staticmethod
    def put_role(label):
        put_role_req = transaction_messages.Transaction.PutRole.Req()
        put_role_req.label = label
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.putRole_req.CopyFrom(put_role_req)
        return transaction_req

    @staticmethod
    def put_rule(label, when, then):
        put_rule_req = transaction_messages.Transaction.PutRule.Req()
        put_rule_req.label = label
        put_rule_req.when = when
        put_rule_req.then = then
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.putRule_req.CopyFrom(put_rule_req)
        return transaction_req


    # --- internal requests ---

    @staticmethod
    def next_iter(iterator_id):
        iterate_request = transaction_messages.Transaction.Iter.Req()
        iterate_request.id = iterator_id
        
        transaction_req = transaction_messages.Transaction.Req()
        transaction_req.iterate_req.CopyFrom(iterate_request)
        return transaction_req


    # ------ Concept Method Requests ------

    class ConceptMethod(object):
        """ Construct Concept Method requests """

        @staticmethod
        def delete():
            delete_req = concept_messages.Concept.Delete.Req()
            concept_method_req = concept_messages.Method.Req()
            concept_method_req.concept_delete_req.CopyFrom(delete_req)
            return concept_method_req

        @staticmethod
        def _concept_to_grpc_concept(concept):
            """ Takes a concept from ConceptHierarcy and converts to GRPC message """
            grpc_concept = concept_messages.Concept()
            grpc_concept.id = concept.id
            base_type_name = concept.base_type
            grpc_base_type = BaseTypeMapping.name_to_grpc_base_type[base_type_name]
            grpc_concept.baseType = grpc_base_type
            return grpc_concept

        def as_value_object(data, datatype):
            msg = concept_messages.ValueObject()
            if datatype == enums.DataType.STRING:
                msg.string = data
            elif datatype == enums.DataType.BOOLEAN:
                msg.boolean = data
            elif datatype == enums.DataType.INTEGER:
                msg.integer = data
            elif datatype == enums.DataType.LONG:
                msg.long = data
            elif datatype == enums.DataType.FLOAT:
                msg.float = data
            elif datatype == enums.DataType.DOUBLE:
                msg.double = data
            elif datatype == enums.DataType.DATE:
                # convert local datetime into long
                epoch = datetime(1970, 1, 1)
                diff = data - epoch
                epoch_seconds_utc = int(diff.total_seconds())
                epoch_ms_long_utc = int(epoch_seconds_utc*1000)
                msg.date = epoch_ms_long_utc 
            else:
                # TODO specialize exception
                raise Exception("Unknown attribute datatype: {}".format(datatype))
            return msg
        as_value_object.__annotations__ = {'datatype': enums.DataType}
        as_value_object = staticmethod(as_value_object)



        class SchemaConcept(object):
            """ Generates SchemaConcept method messages """

            @staticmethod
            def get_label():
                get_schema_label_req = concept_messages.SchemaConcept.GetLabel.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_getLabel_req.CopyFrom(get_schema_label_req)
                return concept_method_req
          
            @staticmethod
            def set_label(label):
                set_schema_label_req = concept_messages.SchemaConcept.SetLabel.Req()
                set_schema_label_req.label = label
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_setLabel_req.CopyFrom(set_schema_label_req)
                return concept_method_req

            @staticmethod
            def is_implicit():
                is_implicit_req = concept_messages.SchemaConcept.IsImplicit.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_isImplicit_req.CopyFrom(is_implicit_req)
                return concept_method_req

            @staticmethod
            def get_sup():
                get_sup_req = concept_messages.SchemaConcept.GetSup.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_getSup_req.CopyFrom(get_sup_req)
                return concept_method_req

            @staticmethod
            def set_sup(concept): 
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(concept)
                set_sup_req = concept_messages.SchemaConcept.SetSup.Req()
                set_sup_req.schemaConcept.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_setSup_req.CopyFrom(set_sup_req)
                return concept_method_req

            @staticmethod
            def subs():
                subs_req = concept_messages.SchemaConcept.Subs.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_subs_req.CopyFrom(subs_req)
                return concept_method_req

            @staticmethod
            def sups():
                sups_req = concept_messages.SchemaConcept.Sups.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.schemaConcept_sups_req.CopyFrom(sups_req)
                return concept_method_req



        class Rule(object):
            """ Generates Rule method messages """

            @staticmethod
            def when():
                when_req = concept_messages.Rule.When.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.rule_when_req.CopyFrom(when_req)
                return concept_method_req

            @staticmethod
            def then():
                then_req = concept_messages.Rule.Then.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.rule_then_req.CopyFrom(then_req)
                return concept_method_req

        class Role(object):
            """ Generates Role method messages """

            @staticmethod
            def relations():
                relations_req = concept_messages.Role.Relations.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.role_relations_req.CopyFrom(relations_req)
                return concept_method_req

            @staticmethod
            def players():
                players_req = concept_messages.Role.Players.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.role_players_req.CopyFrom(players_req)
                return concept_method_req

            

        class Type(object):
            """ Generates Type method messages """

            @staticmethod
            def is_abstract():
                is_abstract_req = concept_messages.Type.IsAbstract.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_isAbstract_req.CopyFrom(is_abstract_req)
                return concept_method_req

            @staticmethod
            def set_abstract(abstract):
                set_abstract_req = concept_messages.Type.SetAbstract.Req()
                assert type(abstract) == bool
                set_abstract_req.abstract = abstract
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_setAbstract_req.CopyFrom(set_abstract_req)
                return concept_method_req

            @staticmethod
            def instances():
                type_instances_req = concept_messages.Type.Instances.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_instances_req.CopyFrom(type_instances_req)
                return concept_method_req

            @staticmethod
            def keys():
                type_keys_req = concept_messages.Type.Keys.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_keys_req.CopyFrom(type_keys_req)
                return concept_method_req

            @staticmethod
            def attributes():
                type_attributes_req = concept_messages.Type.Attributes.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_attributes_req.CopyFrom(type_attributes_req)
                return concept_method_req 

            @staticmethod
            def has(attribute_type_concept):
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_type_concept)
                has_req = concept_messages.Type.Has.Req()
                has_req.attributeType.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_has_req.CopyFrom(has_req)
                return concept_method_req

            @staticmethod
            def unhas(attribute_type_concept):
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_type_concept)
                unhas_req = concept_messages.Type.Unhas.Req()
                unhas_req.attributeType.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_unhas_req.CopyFrom(unhas_req)
                return concept_method_req

            @staticmethod
            def key(attribute_type_concept):
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_type_concept)
                key_req = concept_messages.Type.Key.Req()
                key_req.attributeType.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_key_req.CopyFrom(key_req)
                return concept_method_req

            @staticmethod
            def unkey(attribute_type_concept):
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_type_concept)
                unkey_req = concept_messages.Type.Unkey.Req()
                unkey_req.attributeType.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_unkey_req.CopyFrom(unkey_req)
                return concept_method_req

            @staticmethod
            def playing():
                playing_req = concept_messages.Type.Playing.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_playing_req.CopyFrom(playing_req)
                return concept_method_req

            @staticmethod
            def plays( role_concept):
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                plays_req = concept_messages.Type.Plays.Req()
                plays_req.role.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_plays_req.CopyFrom(plays_req)
                return concept_method_req

            @staticmethod
            def unplay(role_concept):
                grpc_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                unplay_req = concept_messages.Type.Unplay.Req()
                unplay_req.role.CopyFrom(grpc_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.type_unplay_req.CopyFrom(unplay_req)
                return concept_method_req

       


        class EntityType(object):
            """ Generates EntityType method messages """

            @staticmethod
            def create():
                create_req = concept_messages.EntityType.Create.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.entityType_create_req.CopyFrom(create_req)
                return concept_method_req

        class RelationType(object):
            """ Generates RelationType method messages """
            
            @staticmethod
            def create():
                create_req = concept_messages.RelationType.Create.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.relationType_create_req.CopyFrom(create_req)
                return concept_method_req

            @staticmethod
            def roles():
                roles_req = concept_messages.RelationType.Roles.Req()
                concept_messages_req = concept_messages.Method.Req()
                concept_messages_req.relationType_roles_req.CopyFrom(roles_req)
                return concept_messages_req

            @staticmethod
            def relates(role_concept):
                grpc_role_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                relates_req = concept_messages.RelationType.Relates.Req()
                relates_req.role.CopyFrom(grpc_role_concept)
                concept_messages_req = concept_messages.Method.Req()
                concept_messages_req.relationType_relates_req.CopyFrom(relates_req)
                return concept_messages_req

            @staticmethod
            def unrelate(role_concept):
                grpc_role_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                unrelate_req = concept_messages.RelationType.Unrelate.Req()
                unrelate_req.role.CopyFrom(grpc_role_concept)
                concept_messages_req = concept_messages.Method.Req()
                concept_messages_req.relationType_unrelate_req.CopyFrom(unrelate_req)
                return concept_messages_req

        class AttributeType(object):
            """ Generates AttributeType method messages """
            
            @staticmethod
            def create(value, datatype):
                grpc_value_object = RequestBuilder.ConceptMethod.as_value_object(value, datatype)
                create_attr_req = concept_messages.AttributeType.Create.Req()
                create_attr_req.value.CopyFrom(grpc_value_object)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attributeType_create_req.CopyFrom(create_attr_req)
                return concept_method_req

            @staticmethod
            def attribute(value, datatype):
                grpc_value_object = RequestBuilder.ConceptMethod.as_value_object(value, datatype)
                attribute_req = concept_messages.AttributeType.Attribute.Req()
                attribute_req.value.CopyFrom(grpc_value_object)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attributeType_attribute_req.CopyFrom(attribute_req)
                return concept_method_req

            @staticmethod
            def data_type():
                datatype_req = concept_messages.AttributeType.DataType.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attributeType_dataType_req.CopyFrom(datatype_req)
                return concept_method_req

            @staticmethod
            def get_regex():
                get_regex_req = concept_messages.AttributeType.GetRegex.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attributeType_getRegex_req.CopyFrom(get_regex_req)
                return concept_method_req

            @staticmethod
            def set_regex(regex):
                set_regex_req = concept_messages.AttributeType.SetRegex.Req()
                set_regex_req.regex = regex
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attributeType_setRegex_req.CopyFrom(set_regex_req)
                return concept_method_req


        class Thing(object):
            """ Generates Thing method messages """

            @staticmethod
            def is_inferred():
                is_inferred_req = concept_messages.Thing.IsInferred.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_isInferred_req.CopyFrom(is_inferred_req)
                return concept_method_req

            @staticmethod
            def type():
                type_req = concept_messages.Thing.Type.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_type_req.CopyFrom(type_req)
                return concept_method_req
                
            @staticmethod
            def attributes(attribute_types=[]):
                """ Takes a list of AttributeType concepts to narrow attribute retrieval """
                attributes_req = concept_messages.Thing.Attributes.Req()
                for attribute_type_concept in attribute_types:
                    grpc_attr_type_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_type_concept)
                    attributes_req.attributeTypes.extend([grpc_attr_type_concept])
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_attributes_req.CopyFrom(attributes_req)
                return concept_method_req

            @staticmethod
            def relations(role_concepts=[]):
                """ Takes a list of role concepts to narrow the relations retrieval """
                relations_req = concept_messages.Thing.Relations.Req()
                for role_concept in role_concepts:
                    grpc_role_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                    # TODO this could use .add() if can be made to work...
                    relations_req.roles.extend([grpc_role_concept])
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_relations_req.CopyFrom(relations_req)
                return concept_method_req
            
            @staticmethod
            def roles():
                roles_req = concept_messages.Thing.Roles.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_roles_req.CopyFrom(roles_req)
                return concept_method_req

            @staticmethod
            def keys(attribute_types=[]):
                """ Takes a  list of AttributeType concepts to narrow the key retrieval """
                keys_req = concept_messages.Thing.Keys.Req()
                for attribute_type_concept in attribute_types:
                    grpc_attr_type_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_type_concept)
                    keys_req.attributeTypes.extend([grpc_attr_type_concept])
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_keys_req.CopyFrom(keys_req)
                return concept_method_req
            
            @staticmethod
            def has(attribute_concept):
                grpc_attribute_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_concept)
                relhas_req = concept_messages.Thing.Relhas.Req()
                relhas_req.attribute.CopyFrom(grpc_attribute_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_relhas_req.CopyFrom(relhas_req)
                return concept_method_req

            @staticmethod
            def unhas(attribute_concept):
                grpc_attribute_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(attribute_concept)
                unhas_req = concept_messages.Thing.Unhas.Req()
                unhas_req.attribute.CopyFrom(grpc_attribute_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.thing_unhas_req.CopyFrom(unhas_req)
                return concept_method_req
        
        class Relation(object):
            """ Generates Relation (aka Relationship) method messages """

            @staticmethod
            def role_players_map():
                role_players_map_req = concept_messages.Relation.RolePlayersMap.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.relation_rolePlayersMap_req.CopyFrom(role_players_map_req)
                return concept_method_req

            @staticmethod
            def role_players(roles=[]):
                """ Retrieve concepts that can play the given roles """
                role_players_req = concept_messages.Relation.RolePlayers.Req()
                for role_concept in roles:
                    grpc_role_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                    role_players_req.roles.extend([grpc_role_concept])
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.relation_rolePlayers_req.CopyFrom(role_players_req)
                return concept_method_req
    
            @staticmethod
            def assign(role_concept, player_concept):
                grpc_role_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                grpc_player_concept  = RequestBuilder.ConceptMethod._concept_to_grpc_concept(player_concept)
                assign_req = concept_messages.Relation.Assign.Req()
                assign_req.role.CopyFrom(grpc_role_concept)
                assign_req.player.CopyFrom(grpc_player_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.relation_assign_req.CopyFrom(assign_req)
                return concept_method_req
            
            @staticmethod
            def unassign(role_concept, player_concept):
                grpc_role_concept = RequestBuilder.ConceptMethod._concept_to_grpc_concept(role_concept)
                grpc_player_concept  = RequestBuilder.ConceptMethod._concept_to_grpc_concept(player_concept)
                unassign_req = concept_messages.Relation.Unassign.Req()
                unassign_req.role.CopyFrom(grpc_role_concept)
                unassign_req.player.CopyFrom(grpc_player_concept)
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.relation_unassign_req.CopyFrom(unassign_req)
                return concept_method_req

        class Attribute(object):
            """ Generates Attribute method messages """

            @staticmethod
            def value():
                value_req = concept_messages.Attribute.Value.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attribute_value_req.CopyFrom(value_req)
                return concept_method_req

            @staticmethod
            def owners():
                owners_req = concept_messages.Attribute.Owners.Req()
                concept_method_req = concept_messages.Method.Req()
                concept_method_req.attribute_owners_req.CopyFrom(owners_req)
                return concept_method_req
        
        class Entity(object):
            """ Empty implementation -- never create requests on Entity """
            pass
