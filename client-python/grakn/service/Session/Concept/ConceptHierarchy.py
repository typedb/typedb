from typing import Union, Optional

from grakn.service.Session.util import enums
from grakn.service.Session.util.RequestBuilder import RequestBuilder
# from grakn.service.Session.util.ResponseConverter import ResponseConverter # cannot get name from module because circular imports
import grakn.service.Session.util.ResponseConverter as ResponseConverter # toplevel only allowed
from grakn.service.Session.Concept import ConceptFactory



class Concept(object):

    def __init__(self, concept_id, base_type, tx_service):
        self.id = concept_id
        self.base_type = base_type
        self._tx_service = tx_service


    def delete(self):
        del_request = RequestBuilder.ConceptMethod.delete()
        method_response = self._tx_service.run_concept_method(self.id, del_request)
        return

    def is_schema_concept(self) -> bool:
        return isinstance(self, SchemaConcept)

    def is_type(self) -> bool:
        return isinstance(self, Type)

    def is_thing(self) -> bool:
        return isinstance(self, Thing)

    def is_attribute_type(self) -> bool:
        return isinstance(self, AttributeType)

    def is_entity_type(self) -> bool:
        return isinstance(self, EntityType)

    def is_relationship_type(self) -> bool:
        return isinstance(self, RelationshipType)

    def is_role(self) -> bool:
        return isinstance(self, Role)

    def is_rule(self) -> bool:
        return isinstance(self, Rule)

    def is_attribute(self) -> bool:
        return isinstance(self, Attribute)

    def is_entity(self) -> bool:
        return isinstance(self, Entity)

    def is_relationship(self) -> bool:
        return isinstance(self, Relationship)


class SchemaConcept(Concept):

    def label(self, value=None) -> Optional[str]:
        if value is None:
            get_label_req = RequestBuilder.ConceptMethod.SchemaConcept.get_label()
            method_response = self._tx_service.run_concept_method(self.id, get_label_req)
            return method_response.schemaConcept_getLabel_res.label
        else:
            set_label_req = RequestBuilder.ConceptMethod.SchemaConcept.set_label(value)
            method_response = self._tx_service.run_concept_method(self.id, set_label_req)
            return

    def is_implicit(self) -> bool:
        is_implicit_req = RequestBuilder.ConceptMethod.SchemaConcept.is_implicit()
        method_response = self._tx_service.run_concept_method(self.id, is_implicit_req)
        return method_response.implicit

    def sup(self, super_concept=None) -> Optional[Concept]:
        if super_concept is None:
            # get direct super schema concept
            get_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.get_sup()
            method_response = self._tx_service.run_concept_method(self.id, get_sup_req)
            get_sup_response = method_response.schemaConcept_getSup_res 
            # check if received a Null or Concept
            whichone = get_sup_response.WhichOneof('res')
            if whichone == 'schemaConcept':
                grpc_schema_concept = get_sup_response.schemaConcept
                concept = ConceptFactory.create_concept(self._tx_service, grpc_schema_concept)
            elif whichone == 'null':
                return None
            else:
                # TODO specialize exception
                raise Exception("Unknown response concent for getting super schema concept: {0}".format(whichone))
        else:
            # set direct super SchemaConcept of this SchemaConcept
            set_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.set_sup(super_concept)
            method_response = self._tx_service.run_concept_method(self.id, set_sup_req)

    def subs(self):
        subs_req = RequestBuilder.ConceptMethod.SchemaConcept.subs()
        method_response = self._tx_service.run_concept_method(self.id, subs_req)
        return ResponseConverter.ResponseConverter.SchemaConcept.subs_iterator(self._tx_service, method_response.schemaConcept_sups_iter) 

    def sups(self):
        sups_req = RequestBuilder.ConceptMethod.SchemaConcept.sups()
        method_response = self._tx_service.run_concept_method(self.id, sups_req)
        return ResponseConverter.ResponseConverter.SchemaConcept.sups_iterator(self._tx_service, method_response.schemaConcept_sups_iter)

class Type(SchemaConcept):

    def is_abstract(self, value: bool = None) -> Optional[bool]:
        if value is None:
            # return True/False if the type is set to abstract
            is_abstract_req = RequestBuilder.ConceptMethod.Type.is_abstract()
            method_response = self._tx_service.run_concept_method(self.id, is_abstract_req)
            return method_response.type_isAbstract_res.abstract
        else:
            set_abstract_req = RequestBuilder.ConceptMethod.Type.set_abstract(value)
            method_response = self._tx_service.run_concept_method(self.id, set_abstract_req)
            return 

    def attributes(self):
        attributes_req = RequestBuilder.ConceptMethod.Type.attributes()
        method_response = self._tx_service.run_concept_method(self.id, attributes_req)
        return ResponseConverter.ResponseConverter.Type.attributes(self._tx_service, method_response.type_attributes_iter)

    def instances(self):
        instances_req = RequestBuilder.ConceptMethod.Type.instances()
        method_response = self._tx_service.run_concept_method(self.id, instances_req)
        return ResponseConverter.ResponseConverter.Type.instances(self._tx_service, method_response.type_instances_iter)

    def playing(self):
        playing_req = RequestBuilder.ConceptMethod.Type.playing()
        method_response = self._tx_service.run_concept_method(self.id, playing_req)
        return ResponseConverter.ResponseConverter.Type.playing(self._tx_service, method_response.type_playing_iter)

    def plays(self, role_concept):
        plays_req = RequestBuilder.ConceptMethod.Type.plays(role_concept)
        method_response = self._tx_service.run_concept_method(self.id, plays_req)
        return

    def unplay(self, role_concept):
        unplay_req = RequestBuilder.ConceptMethod.Type.unplays(role_concept)
        method_response = self._tx_service.run_concept_method(self.id, unplay_req)
        return
    
    def has(self, attribute_concept_type):
        """ Attach a attributeType concept to the type """
        has_req = RequestBuilder.ConceptMethod.Type.has(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, has_req)
        return
        
    def unhas(self, attribute_concept_type):
        unhas_req = RequestBuilder.ConceptMethod.Type.unhas(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, unhas_req)
        return 

    def keys(self):
        keys_req = RequestBuilder.ConceptMethod.Type.keys()
        method_response = self._tx_service.run_concept_method(self.id, keys_req)
        return ResponseConverter.ResponseConverter.Type.keys(self._tx_service, method_response.type_keys_iter) 


    def key(self, attribute_concept_type):
        key_req = RequestBuilder.ConceptMethod.Type.key(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, key_req)
        return

    def unkey(self, attribute_concept_type):
        unkey_req = RequestBuilder.ConceptMethod.Type.unkey(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, unkey_req)
        return



class EntityType(Type):

    def create(self):
        """ Instantiate an entity of the given type and return it """
        create_req = RequestBuilder.ConceptMethod.EntityType.create()
        method_response = self._tx_service.run_concept_method(self.id, create_req)
        grpc_entity_concept = method_response.entityType_create_res.entity
        return ConceptFactory.create_concept(self._tx_service, grpc_entity_concept)

class AttributeType(Type):
    
    def create(self, value):
        """ Create an instance with this AttributeType """
        self_data_type: enums.DataType = self.data_type()
        create_inst_req = RequestBuilder.ConceptMethod.AttributeType.create(value, self_data_type)
        method_response = self._tx_service.run_concept_method(self.id, create_inst_req)
        grpc_attribute_concept = method_response.attributeType_create_res.attribute
        return ConceptFactory.create_concept(self._tx_service, grpc_attribute_concept)
        
    def attribute(self, value):
        self_data_type = self.data_type()
        get_attribute_req = RequestBuilder.ConceptMethod.AttributeType.attribute(value, self_data_type)
        method_response = self._tx_service.run_concept_method(self.id, get_attribute_req)
        response = method_response.attributeType_attribute_res
        whichone = response.WhichOneof('res')
        if whichone == 'attribute':
            return ConceptFactory.create_concept(self._tx_service, response.attribute)
        elif whichone == 'null':
            return None
        else:
            raise Exception("Unknown `res` key in AttributeType `attribute` response: {0}".format(whichone))

    def data_type(self):
        get_data_type_req = RequestBuilder.ConceptMethod.AttributeType.data_type()
        method_response = self._tx_service.run_concept_method(self.id, get_data_type_req)
        response = method_response.attributeType_dataType_res
        whichone = response.WhichOneof('res')
        if whichone == 'dataType':
            # iterate over enum DataType enum to find matching data type
            for e in enums.DataType:
                if e.value == response.dataType:
                    return e
            else:
                # loop exited normally
                # didn't find datatype...
                # TODO specialize exception
                raise Exception("Reported datatype NOT in enum: {0}".format(response.dataType))
        elif whichone == 'null':
            return None
        else:
            raise Exception("Unknown datatype response for AttributeType: {0}".format(whichone))

    def regex(self, pattern: str = None):
        if pattern is None:
            get_regex_req = RequestBuilder.ConceptMethod.AttributeType.get_regex()
            method_response = self._tx_service.run_concept_method(self.id, get_regex_req)
            return method_response.attributeType_getRegex_res.regex
        else:
            set_regex_req = RequestBuilder.ConceptMethod.AttributeType.set_regex(pattern)
            method_response = self._tx_service.run_concept_method(self.id, set_regex_req)
            return


class RelationshipType(Type):

    # NOTE: `relation` not `relationship` used in RequestBuilder already 
    
    def create(self):
        """ Create an instance of a relationship with this type """
        create_rel_inst_req = RequestBuilder.ConceptMethod.RelationType.create()
        method_response = self._tx_service.run_concept_method(self.id, create_rel_inst_req)
        grpc_relationship_concept = method_response.relationshipType_create_res.relation
        return ConceptFactory.create_concept(self._tx_service, grpc_relationship_concept)
        
    def roles(self):
        get_roles = RequestBuilder.ConceptMethod.RelationType.roles()
        method_response = self._tx_service.run_concept_method(self.id, get_roles)
        return ResponseConverter.ResponseConverter.RelationshipType.roles(self._tx_service, method_response.relationType_roles_iter)
        

    def relates(self, role):
        relates_req = RequestBuilder.ConceptMethod.RelationType.relates(role)
        method_response = self._tx_service.run_concept_method(self.id, relates_req)
        return
        

    def unrelate(self, role):
        unrelate_req = RequestBuilder.ConceptMethod.RelationType.relates(role)
        method_response = self._tx_service.run_concept_method(self.id, unrelate_req)
        return

class Rule(SchemaConcept):

    def get_when(self):
        when_req = RequestBuilder.ConceptMethod.Rule.when()
        method_response = self._tx_service.run_concept_method(self.id, when_req)
        response = method_response.rule_when_res
        whichone = response.WhichOneof('res')
        if whichone == 'pattern':
            return response.pattern
        elif whichone == 'null':
            return None
        else:
            raise Exception("Unknown field in get_when of `rule`: {0}".format(whichone))

    def get_then(self):
        then_req = RequestBuilder.ConceptMethod.Rule.then()
        method_response = self._tx_service.run_concept_method(self.id, then_req)
        response = method_response.rule.then_res
        whichone = response.WhichOneof('res')
        if whichone == 'pattern':
            return response.pattern
        elif whichone == 'null':
            return None
        else:
            raise Exception("Unknown field in get_then or `rule`: {0}".format(whichone))

class Role(SchemaConcept):

    def relationships(self):
        # NOTE: relations vs relationships here
        relations_req = RequestBuilder.ConceptMethod.Role.relations()
        method_response = self._tx_service.run_concept_method(self.id, relations_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.role_relations_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.role_relations_iter_res.relationType)
               )

    def players(self):
        players_req = RequestBuilder.ConceptMethod.Role.players()
        method_response = self._tx_service.run_concept_method(self.id, players_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.role_players_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.role_players_iter_res.type)
               )


class Thing(Concept):

    def is_inferred(self) -> bool:
        is_inferred_req = RequestBuilder.ConceptMethod.Thing.is_inferred()
        method_response = self._tx_service.run_concept_method(self.id, is_inferred_req)
        return method_response.thing_isInferred_res.inferred

    def type(self):
        type_req = RequestBuilder.ConceptMethod.Thing.type()
        method_response = self._tx_service.run_concept_method(self.id, type_req)
        return ConceptFactory.create_concept(self._tx_service, method_response.thing_type_res.type)

    def relationships(self, *roles):
        # NOTE `relations` rather than `relationships`
        relations_req = RequestBuilder.ConceptMethod.Thing.relations(roles)
        method_response = self._tx_service.run_concept_method(self.id, relations_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_relations_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_relations_iter_res.relation)
               )

    def attributes(self, *attribute_types):
        attrs_req = RequestBuilder.ConceptMethod.Thing.attributes(attribute_types)
        method_response = self._tx_service.run_concept_method(self.id, attrs_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_attributes_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_attributes_iter_res.attribute)
               )

    def plays(self):
        plays_req = RequestBuilder.ConceptMethod.Thing.plays()
        method_response = self._tx_service.run_concept_method(self.id, plays_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_plays_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_plays_iter_res.role)
               )

    def keys(self, *attribute_types):
        keys_req = RequestBuilder.ConceptMethod.Thing.keys(attribute_types)
        method_response = self._tx_service.run_concept_method(self.id, keys_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_keys_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_keys_iter_res.attribute)
               )


    def has(self, attribute):
        has_req = RequestBuilder.ConceptMethod.Thing.has(attribute)
        method_response = self._tx_service.run_concept_method(self.id, has_req)
        return


    def unhas(self, attribute):
        unhas_req = RequestBuilder.ConceptMethod.Thing.unhas(attribute)
        method_response = self._tx_service.run_concept_method(self.id, unhas_req)
        return 


class Entity(Thing):
    pass

class Attribute(Thing):

    def value(self):
        value_req = RequestBuilder.ConceptMethod.Attribute.value()
        method_response = self._tx_service.run_concept_method(self.id, value_req)
        grpc_value_object = method_response.attribute_value_res.value
        return ResponseConverter.ResponseConverter.from_grpc_value_object(grpc_value_object)

    def owners(self):
        owners_req = RequestBuilder.ConceptMethod.Attribute.owners()
        method_response = self._tx_service.run_concept_method(self.id, owners_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.attribute_ownser_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.attribute_owners_iter_res.thing)
               )

class Relationship(Thing):

    # NOTE `relation` has replaced `relationship` in ResponseBuilder

    def role_players_map(self):
        role_players_map_req = RequestBuilder.ConceptMethod.Relation.role_players_map()
        method_response = self._tx_service.run_concept_method(self.id, role_players_map_req)

        # create the iterator to obtain all the pairs of (role, player)
        def to_pair(tx_service, iter_res):
            response = iter_res.conceptMethod_iter_res.relationship_rolePlayersMap_iter_res
            role = ConceptFactory.create_concept(tx_service, response.role)
            player = ConceptFactory.create_concept(tx_service, response.player)
            return (role, player)

        iterator = ResponseConverter.ResponseConverter.iter_res_to_iterator(
                    self._tx_service,
                    method_response.relation_rolePlayersMap_iter.id,
                    to_pair)
        
        # collect all pairs of (role, player) from the iterator (executes over network to Grakn server)
        pairs = list(iterator)
        
        # aggregate into a map from role to set(player)
        # note: need to use role ID as the map key ultimately
        mapping = {}
        id_mapping = {}
        for (role, player) in pairs:
            role_id = role.id
            if role_id in id_mapping:
                role_key = id_mapping[role_id]
                # if role key is not in id_mapping, its also not in mapping!
                mapping[role_key] = set()
            else:
                id_mapping[role_id] = role
                role_key = role
            mapping[role_key].add(player)

        return mapping

    def role_players(self, *roles):
        role_players_req = RequestBuilder.ConceptMethod.Relation.role_players()
        method_response = self._tx_service.run_concept_method(self.id, role_players_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.relation_rolePlayer_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.relation_rolePlayers_iter_res.thing)
               )


    def assign(self, role, thing):
        assign_req = RequestBuilder.ConceptMethod.Relation.assign(role, thing)
        method_response = self._tx_service.run_concept_method(self.id, assign_req)
        return

    def unassign(self, role, thing):
        unassign_req = RequestBuilder.ConceptMethod.Relation.unassign(role, thing)
        method_response = self._tx_service.run_concept_method(self.id, unassign_req)
        return
