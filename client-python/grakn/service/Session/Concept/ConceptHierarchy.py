from typing import Union, Optional

from grakn.service.Session.util import enums
from grakn.service.Session.util.RequestBuilder import RequestBuilder
# from grakn.service.Session.util.ResponseConverter import ResponseConverter # cannot get name from module because circular imports
# import grakn.service.Session.util.ResponseConverter as ResponseConverter # toplevel only allowed
from grakn.service.Session.util import ResponseConverter
from grakn.service.Session.Concept import ConceptFactory
from grakn.exception.ClientError import ClientError 



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
        """ Check if this concept is a schema concept """
        return isinstance(self, SchemaConcept)

    def is_type(self) -> bool:
        """ Check if this concept is a Type concept """
        return isinstance(self, Type)

    def is_thing(self) -> bool:
        """ Check if this concept is a Thing concept """
        return isinstance(self, Thing)

    def is_attribute_type(self) -> bool:
        """ Check if this concept is an AttributeType concept """
        return isinstance(self, AttributeType)

    def is_entity_type(self) -> bool:
        """ Check if this concept is an EntityType concept """
        return isinstance(self, EntityType)

    def is_relationship_type(self) -> bool:
        """ Check if this concept is a RelationshipType concept """
        return isinstance(self, RelationshipType)

    def is_role(self) -> bool:
        """ Check if this concept is a Role """
        return isinstance(self, Role)

    def is_rule(self) -> bool:
        """ Check if this concept is a Rule concept """
        return isinstance(self, Rule)

    def is_attribute(self) -> bool:
        """ Check if this concept is an Attribute concept """
        return isinstance(self, Attribute)

    def is_entity(self) -> bool:
        """ Check if this concept is an Entity concept """
        return isinstance(self, Entity)

    def is_relationship(self) -> bool:
        """ Check if this concept is a Relationship concept """
        return isinstance(self, Relationship)


class SchemaConcept(Concept):

    def label(self, value=None):
        """ 
        Get or set label of this schema concept. 
        If used as setter returns self 
        """
        if value is None:
            get_label_req = RequestBuilder.ConceptMethod.SchemaConcept.get_label()
            method_response = self._tx_service.run_concept_method(self.id, get_label_req)
            return method_response.schemaConcept_getLabel_res.label
        else:
            set_label_req = RequestBuilder.ConceptMethod.SchemaConcept.set_label(value)
            method_response = self._tx_service.run_concept_method(self.id, set_label_req)
            return self

    def is_implicit(self):
        """ Check if this schema concept is implicit """
        is_implicit_req = RequestBuilder.ConceptMethod.SchemaConcept.is_implicit()
        method_response = self._tx_service.run_concept_method(self.id, is_implicit_req)
        return method_response.schemaConcept_isImplicit_res.implicit

    def sup(self, super_concept=None):
        """ 
        Get or set super schema concept.
        If used as a setter returns self
        """
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
                return concept
            elif whichone == 'null':
                return None
            else:
                raise ClientError("Unknown response concent for getting super schema concept: {0}".format(whichone))
        else:
            # set direct super SchemaConcept of this SchemaConcept
            set_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.set_sup(super_concept)
            method_response = self._tx_service.run_concept_method(self.id, set_sup_req)
            return self

    def subs(self):
        """ Retrieve the sub schema concepts of this schema concept, as an iterator """
        subs_req = RequestBuilder.ConceptMethod.SchemaConcept.subs()
        method_response = self._tx_service.run_concept_method(self.id, subs_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                    self._tx_service,
                    method_response.schemaConcept_subs_iter.id,
                    lambda tx_serv, iter_res: 
                        ConceptFactory.create_concept(tx_serv,  
                        iter_res.conceptMethod_iter_res.schemaConcept_subs_iter_res.schemaConcept)
                    )

    def sups(self):
        """ Retrieve the all supertypes (direct and higher level) of this schema concept as an iterator """
        sups_req = RequestBuilder.ConceptMethod.SchemaConcept.sups()
        method_response = self._tx_service.run_concept_method(self.id, sups_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.schemaConcept_sups_iter.id,
                lambda tx_serv, iter_res:
                    ConceptFactory.create_concept(tx_serv, 
                    iter_res.conceptMethod_iter_res.schemaConcept_sups_iter_res.schemaConcept)
                )
 
class Type(SchemaConcept):

    def is_abstract(self, value: bool = None) -> Optional[bool]:
        """
        Get/Set whether this schema Type object is abstract.
        When used as a setter returns `self` 
        """
        if value is None:
            # return True/False if the type is set to abstract
            is_abstract_req = RequestBuilder.ConceptMethod.Type.is_abstract()
            method_response = self._tx_service.run_concept_method(self.id, is_abstract_req)
            return method_response.type_isAbstract_res.abstract
        else:
            set_abstract_req = RequestBuilder.ConceptMethod.Type.set_abstract(value)
            method_response = self._tx_service.run_concept_method(self.id, set_abstract_req)
            return self

    def attributes(self):
        """ Retrieve all attributes attached to this Type as an iterator """
        attributes_req = RequestBuilder.ConceptMethod.Type.attributes()
        method_response = self._tx_service.run_concept_method(self.id, attributes_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.type_attributes_iter.id,
                lambda tx_serv, iter_res: 
                    ConceptFactory.create_concept(tx_serv,
                    iter_res.conceptMethod_iter_res.type_attributes_iter_res.attributeType)
                )

    def instances(self):
        """ Retrieve all instances of this Type as an iterator """
        instances_req = RequestBuilder.ConceptMethod.Type.instances()
        method_response = self._tx_service.run_concept_method(self.id, instances_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.type_instances_iter.id,
                lambda tx_serv, iter_res: 
                    ConceptFactory.create_concept(tx_serv,
                    iter_res.conceptMethod_iter_res.type_instances_iter_res.thing)
                )

    def playing(self):
        """ Retrieve iterator of roles played by this type """
        playing_req = RequestBuilder.ConceptMethod.Type.playing()
        method_response = self._tx_service.run_concept_method(self.id, playing_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.type_playing_iter.id,
                lambda tx_serv, iter_res:
                    ConceptFactory.create_concept(tx_serv,
                    iter_res.conceptMethod_iter_res.type_playing_iter_res.role)
                )

    def plays(self, role_concept):
        """ Set a role that is played by this Type """
        plays_req = RequestBuilder.ConceptMethod.Type.plays(role_concept)
        method_response = self._tx_service.run_concept_method(self.id, plays_req)
        return self

    def unplay(self, role_concept):
        """ Remove a role that is played by this Type """
        unplay_req = RequestBuilder.ConceptMethod.Type.unplay(role_concept)
        method_response = self._tx_service.run_concept_method(self.id, unplay_req)
        return
    
    def has(self, attribute_concept_type):
        """ Attach an attributeType concept to the type """
        has_req = RequestBuilder.ConceptMethod.Type.has(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, has_req)
        return self
        
    def unhas(self, attribute_concept_type):
        """ Remove an attribute type concept from this type """
        unhas_req = RequestBuilder.ConceptMethod.Type.unhas(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, unhas_req)
        return self

    def keys(self):
        """ Retrieve an iterator of attribute types that this Type uses as keys """
        keys_req = RequestBuilder.ConceptMethod.Type.keys()
        method_response = self._tx_service.run_concept_method(self.id, keys_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.type_keys_iter.id,
                lambda tx_serv, iter_res:
                    ConceptFactory.create_concept(tx_serv,
                    iter_res.conceptMethod_iter_res.type_keys_iter_res.attributeType)
                )


    def key(self, attribute_concept_type):
        """ Add an attribute type to be a key for this Type """
        key_req = RequestBuilder.ConceptMethod.Type.key(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, key_req)
        return self

    def unkey(self, attribute_concept_type):
        """ Remove an attribute type from this Type from being a key """
        unkey_req = RequestBuilder.ConceptMethod.Type.unkey(attribute_concept_type)
        method_response = self._tx_service.run_concept_method(self.id, unkey_req)
        return self



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
        """ Retrieve an attribute instance by value if it exists """
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
            raise ClientError("Unknown `res` key in AttributeType `attribute` response: {0}".format(whichone))

    def data_type(self):
        """ Get the DataType enum (grakn.DataType) corresponding to the type of this attribute """
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
                raise ClientError("Reported datatype NOT in enum: {0}".format(response.dataType))
        elif whichone == 'null':
            return None
        else:
            raise ClientError("Unknown datatype response for AttributeType: {0}".format(whichone))

    def regex(self, pattern: str = None):
        """ Get or set regex """
        if pattern is None:
            get_regex_req = RequestBuilder.ConceptMethod.AttributeType.get_regex()
            method_response = self._tx_service.run_concept_method(self.id, get_regex_req)
            return method_response.attributeType_getRegex_res.regex
        else:
            set_regex_req = RequestBuilder.ConceptMethod.AttributeType.set_regex(pattern)
            method_response = self._tx_service.run_concept_method(self.id, set_regex_req)
            return self


class RelationshipType(Type):

    # NOTE: `relation` not `relationship` used in RequestBuilder already 
    
    def create(self):
        """ Create an instance of a relationship with this type """
        create_rel_inst_req = RequestBuilder.ConceptMethod.RelationType.create()
        method_response = self._tx_service.run_concept_method(self.id, create_rel_inst_req)
        grpc_relationship_concept = method_response.relationType_create_res.relation
        return ConceptFactory.create_concept(self._tx_service, grpc_relationship_concept)
        
    def roles(self):
        """ Retrieve roles in this relationship schema type """
        get_roles = RequestBuilder.ConceptMethod.RelationType.roles()
        method_response = self._tx_service.run_concept_method(self.id, get_roles)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.relationType_roles_iter.id,
                lambda tx_serv, iter_res:
                    ConceptFactory.create_concept(tx_serv,
                    iter_res.conceptMethod_iter_res.relationType_roles_iter_res.role)
                )
        

    def relates(self, role):
        """ Set a role in this relationship schema type """
        relates_req = RequestBuilder.ConceptMethod.RelationType.relates(role)
        method_response = self._tx_service.run_concept_method(self.id, relates_req)
        return self       

    def unrelate(self, role):
        """ Remove a role in this relationship schema type """
        unrelate_req = RequestBuilder.ConceptMethod.RelationType.unrelate(role)
        method_response = self._tx_service.run_concept_method(self.id, unrelate_req)
        return self

class Rule(SchemaConcept):

    def get_when(self):
        """ Retrieve the `when` clause for this rule """
        when_req = RequestBuilder.ConceptMethod.Rule.when()
        method_response = self._tx_service.run_concept_method(self.id, when_req)
        response = method_response.rule_when_res
        whichone = response.WhichOneof('res')
        if whichone == 'pattern':
            return response.pattern
        elif whichone == 'null':
            return None
        else:
            raise ClientError("Unknown field in get_when of `rule`: {0}".format(whichone))

    def get_then(self):
        """ Retrieve the `then` clause for this rule """
        then_req = RequestBuilder.ConceptMethod.Rule.then()
        method_response = self._tx_service.run_concept_method(self.id, then_req)
        response = method_response.rule_then_res
        whichone = response.WhichOneof('res')
        if whichone == 'pattern':
            return response.pattern
        elif whichone == 'null':
            return None
        else:
            raise ClientError("Unknown field in get_then or `rule`: {0}".format(whichone))

class Role(SchemaConcept):

    def relationships(self):
        """ Retrieve relationships that this role participates in, as an iterator """
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
        """ Retrieve an iterator of entities that play this role """
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
        """ Is this instance inferred """
        is_inferred_req = RequestBuilder.ConceptMethod.Thing.is_inferred()
        method_response = self._tx_service.run_concept_method(self.id, is_inferred_req)
        return method_response.thing_isInferred_res.inferred

    def type(self):
        """ Get the type (schema concept) of this Thing """
        type_req = RequestBuilder.ConceptMethod.Thing.type()
        method_response = self._tx_service.run_concept_method(self.id, type_req)
        return ConceptFactory.create_concept(self._tx_service, method_response.thing_type_res.type)

    def relationships(self, *roles):
        """ Get iterator this Thing's relationships, filtered to the optionally provided roles """
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
        """ Retrieve iterator of this Thing's attributes, filtered by optionally provided attribute types """
        attrs_req = RequestBuilder.ConceptMethod.Thing.attributes(attribute_types)
        method_response = self._tx_service.run_concept_method(self.id, attrs_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_attributes_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_attributes_iter_res.attribute)
               )

    def roles(self):
        """ Retrieve iterator of roles this Thing plays """
        roles_req = RequestBuilder.ConceptMethod.Thing.roles()
        method_response = self._tx_service.run_concept_method(self.id, roles_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_roles_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_roles_iter_res.role)
               )

    def keys(self, *attribute_types):
        """ Retrieve iterator of keys (i.e. actual attributes) of this Thing, filtered by the optionally provided attribute types """
        keys_req = RequestBuilder.ConceptMethod.Thing.keys(attribute_types)
        method_response = self._tx_service.run_concept_method(self.id, keys_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.thing_keys_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.thing_keys_iter_res.attribute)
               )


    def has(self, attribute):
        """ Attach an attribute instance to this Thing """
        has_req = RequestBuilder.ConceptMethod.Thing.has(attribute)
        method_response = self._tx_service.run_concept_method(self.id, has_req)
        return


    def unhas(self, attribute):
        """ Remove an attribute instance from this Thing """
        unhas_req = RequestBuilder.ConceptMethod.Thing.unhas(attribute)
        method_response = self._tx_service.run_concept_method(self.id, unhas_req)
        return 


class Entity(Thing):
    pass

class Attribute(Thing):

    def value(self):
        """ Retrieve the value contained in this Attribute instance """
        value_req = RequestBuilder.ConceptMethod.Attribute.value()
        method_response = self._tx_service.run_concept_method(self.id, value_req)
        grpc_value_object = method_response.attribute_value_res.value
        return ResponseConverter.ResponseConverter.from_grpc_value_object(grpc_value_object)

    def owners(self):
        """ Retrieve entities that have this attribute value """
        owners_req = RequestBuilder.ConceptMethod.Attribute.owners()
        method_response = self._tx_service.run_concept_method(self.id, owners_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.attribute_owners_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.attribute_owners_iter_res.thing)
               )

class Relationship(Thing):

    # NOTE `relation` has replaced `relationship` in ResponseBuilder

    def role_players_map(self):
        """ Retrieve dictionary {role : set(players)} for this relationship """ 
        role_players_map_req = RequestBuilder.ConceptMethod.Relation.role_players_map()
        method_response = self._tx_service.run_concept_method(self.id, role_players_map_req)

        # create the iterator to obtain all the pairs of (role, player)
        def to_pair(tx_service, iter_res):
            response = iter_res.conceptMethod_iter_res.relation_rolePlayersMap_iter_res
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
            else:
                id_mapping[role_id] = role
                role_key = role
                mapping[role_key] = set()
            mapping[role_key].add(player)

        return mapping

    def role_players(self, *roles):
        """ Retrieve role players filtered by roles """
        role_players_req = RequestBuilder.ConceptMethod.Relation.role_players(roles)
        method_response = self._tx_service.run_concept_method(self.id, role_players_req)
        return ResponseConverter.ResponseConverter.iter_res_to_iterator(
                self._tx_service,
                method_response.relation_rolePlayers_iter.id,
                lambda tx_service, iter_res:
                    ConceptFactory.create_concept(tx_service, iter_res.conceptMethod_iter_res.relation_rolePlayers_iter_res.thing)
               )


    def assign(self, role, thing):
        """ Assign an entity to a role on this relationship instance """
        assign_req = RequestBuilder.ConceptMethod.Relation.assign(role, thing)
        method_response = self._tx_service.run_concept_method(self.id, assign_req)
        return self

    def unassign(self, role, thing):
        """ Un-assign an entity from a role on this relationship instance """
        unassign_req = RequestBuilder.ConceptMethod.Relation.unassign(role, thing)
        method_response = self._tx_service.run_concept_method(self.id, unassign_req)
        return self
