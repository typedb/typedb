from typing import Union, Optional
from ..util.RequestBuilder import RequestBuilder
# from . import ResponseConverter
from ..Concept import ConceptFactory



class Concept(object):

    def __init__(self, concept_id, base_type, tx_service):
        self.id = concept_id
        self.base_type = base_type
        self._tx_service = tx_service


    def delete(self):
        # TODO
        pass


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
            response = self._tx_service.run_concept_method(self.id, get_label_req)
            return response.label
        else:
            set_label_req = RequestBuilder.ConceptMethod.SchemaConcept.set_label(value)
            response = self._tx_service.run_concept_method(self.id, set_label_req)
            return

    def is_implicit(self) -> bool:
        is_implicit_req = RequestBuilder.ConceptMethod.SchemaConcept.is_implicit()
        response = self._tx_service.run_concept_method(self.id, is_implicit_req)
        return response.implicit

    def sup(self, super_concept=None) -> Optional[Concept]:
        if super_concept is None:
            # get direct super schema concept
            get_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.get_sup()
            response = self._tx_service.run_concept_method(self.id, get_sup_req)
            
            # check if received a Null or Concept
            whichone = response.WhichOneof('res')
            if whichone == 'schemaConcept':
                grpc_schema_concept = response.schemaConcept
                concept = ConceptFactory.create_concept(self._tx_service, grpc_schema_concept)
            elif whichone == 'null':
                return None
            else:
                # TODO specialize exception
                raise Exception("Unknown response concent for getting super schema concept: {0}".format(whichone))
        else:
            # set direct super SchemaConcept of this SchemaConcept
            set_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.set_sup(super_concept)
            response = self._tx_service.run_concept_method(self.id, set_sup_req)
            return

    def subs(self):
        subs_req = RequestBuilder.ConceptMethod.SchemaConcept.subs()
        response = self._tx_service.run_concept_method(self.id, subs_req)
        # TODO unpack response

    def sups(self):
        sups_req = RequestBuilder.ConceptMethod.SchemaConcept.sups()
        response = self._tx_service.run_concept_method(self.id, sups_req)
        # TODO unpack resposne

class Type(SchemaConcept):

    def is_abstract(self, value: bool = None):
        if value is None:
            # return True/False if the type is set to abstract
            is_abstract_req = RequestBuilder.ConceptMethod.Type.is_abstract()
            response = self._tx_service.run_concept_method(self.id, is_abstract_req)
            # TODO unpack response
        else:
            set_abstract_req = RequestBuilder.ConceptMethod.Type.set_abstract(value)
            response = self._tx_service.run_concept_method(self.id, set_abstract_req)
            # TODO unpack response

    def attributes(self):
        attributes_req = RequestBuilder.ConceptMethod.Type.attributes()
        response = self._tx_service.run_concept_method(self.id, attributes_req)
        # TODO unpack response

    def instances(self):
        instances_req = RequestBuilder.ConceptMethod.Type.instances()
        response = self._tx_service.run_concept_method(self.id, instances_req)
        # TODO unpack response

    def playing(self):
        playing_req = RequestBuilder.ConceptMethod.Type.playing()
        response = self._tx_service.run_concept_method(self.id, playing_req)
        # TODO unpack response

    def plays(self, role_concept):
        plays_req = RequestBuilder.ConceptMethod.Type.plays(role_concept)
        response = self._tx_service.run_concept_method(self.id, plays_req)
        # TODO unpack response

    def unplay(self, role_concept):
        unplay_req = RequestBuilder.ConceptMethod.Type.unplay(role_concept)
        response = self._tx_service.run_concept_method(self.id, unplay_req)
        # TODO unpack response
    
    def has(self, attribute_concept):
        has_req = RequestBuilder.ConceptMethod.Type.has(attribute_concept)
        response = self._tx_service.run_concept_method(self.id, has_req)
        # TODO unpack response

    def unhas(self, attribute_concept):
        unhas_req = RequestBuilder.ConceptMethod.Type.unhas(attribute_concept)
        response = self._tx_service.run_concept_method(self.id, unhas_req)
        # TODO unpack response
        pass

    def keys(self):
        keys_req = RequestBuilder.ConceptMethod.Type.keys()
        response = self._tx_service.run_concept_method(self.id, keys_req)
        # TODO unpack response

    def key(self, attribute_concept):
        key_req = RequestBuilder.ConceptMethod.Type.key(attribute_concept)
        response = self._tx_service.run_concept_method(self.id, key_req)
        # TODO unpack response

    def unkey(self, attribute_concept):
        unkey_req = RequestBuilder.ConceptMethod.Type.unkey(attribute_concept)
        response = self._tx_service.run_concept_method(self.id, unkey_req)
        # TODO unpack response



class EntityType(Type):

    def create(self):
        create_req = RequestBuilder.ConceptMethod.EntityType.create_req()
        response = self._tx_service.run_concept_method(create_req)
        # TODO unpack response

class AttributeType(Type):
    
    def create(self, value):
        """ Create an instance with this AttributeType """
        self_data_type = self.data_type()
        create_inst_req = RequestBuilder.ConceptMethod.AttributeType.create_req(value, self_data_type)
        response = self._tx_service.run_concept_method(create_inst_req)
        # TODO unpack 
        
    def attribute(self, value):
        self_data_type = self.data_type()
        get_attribute_req = RequestBuilder.ConceptMethod.AttributeType.attribute(value, self_data_type)
        response = self._tx_service.run_concept_method(get_attribute_req)
        # TODO unpack

    def data_type(self):
        get_data_type_req = RequestBuilder.ConceptMethod.AttributeType.data_type()
        response =  self._tx_service.run_concept_method(get_data_type_req)
        # TODO unpack

    def regex(self, pattern: str = None):
        if pattern is None:
            get_regex_req = RequestBuilder.ConceptMethod.AttributeType.get_regex()
            response = self._tx_service.run_concept_method(get_regex_req)
            # TODO unpack
        else:
            set_regex_req = RequestBuilder.ConceptMethod.putAttribtueType_req.set_regex(pattern)
            response = self._tx_service.run_concept_method(set_regex_req)
            # TODO unpack


class RelationshipType(Type):

    # NOTE: `relation` not `relationship` in builder already
    def create(self):
        create_rel_inst_req = RequestBuilder.ConceptMethod.RelationType.create()
        response = self._tx_service.run_concept_method(create_rel_inst_req)
        # TODO unpack
        
    def roles(self):
        get_roles = RequestBuilder.ConceptMethod.RelationType.roles()
        response = self._tx_service.run_concept_method(get_roles)
        # TODO unpack

    def relates(self, role: Role):
        relates_req = RequestBuilder.ConceptMethod.RelationType.relates(role)
        response = self._tx_service.run_concept_method(relates_req)
        # TODO unpack
        

    def unrelate(self, role):
        unrelate_req = RequestBuilder.ConceptMethod.RelationType.relates(role)
        response = self._tx_service.run_concept_method(unrelate_req)
        # TODO unpack

class Rule(SchemaConcept):

    def get_when(self):
        when_req = RequestBuilder.ConceptMethod.Rule.when()
        response = self._tx_service.run_concept_method(self.id, when_req)
        # TODO unpack response


    def get_then(self):
        then_req = RequestBuilder.ConceptMethod.Rule.then()
        response = self._tx_service.run_concept_method(self.id, then_req)
        # TODO unpack response

class Role(SchemaConcept):

    def relationships(self):
        # NOTE: relations vs relationships here
        relations_req = RequestBuilder.ConceptMethod.Role.relations()
        response = self._tx_service.run_concept_method(self.id, relations_req)
        # TODO unpack response
        

    def players(self):
        players_req = RequestBuilder.ConceptMethod.Role.players()
        response = self._tx_service.run_concept_method(self.id, players_req)
        # TODO unpack response


class Thing(Concept):

    def is_inferred(self) -> bool:
        is_inferred_req = RequestBuilder.ConceptMethod.Thing.is_inferred()
        response = self._tx_service.run_concept_method(self.id, is_inferred_req)
        # TODO unpack response

    def type(self):
        type_req = RequestBuilder.ConceptMethod.Thing.type()
        response = self._tx_service.run_concept_method(self.id, type_req)
        # TODO unpack response

    def relationships(self, *roles):
        # NOTE `relations` rather than `relationships`
        relations_req = RequestBuilder.ConceptMethod.Thing.relations(roles)
        response = self._tx_service.run_concept_method(self.id, relations_req)
        # TODO unpack response

    def attributes(self, *attribute_types):
        attrs_req = RequestBuilder.ConceptMethod.Thing.attributes(attribute_types)
        response = self._tx_service.run_concept_method(self.id, attrs_req)
        # TODO unpack response

    def plays(self):
        plays_req = RequestBuilder.ConceptMethod.Thing.plays()
        response = self._tx_service.run_concept_method(self.id, plays_req)
        # TODO unpack response

    def keys(self, *attribute_types):
        keys_req = RequestBuilder.ConceptMethod.Thing.keys(attribute_types)
        response = self._tx_service.run_concept_method(self.id, keys_req)
        # TODO unpack response

    def has(self, attribute):
        has_req = RequestBuilder.ConceptMethod.Thing.has(attribute)
        response = self._tx_service.run_concept_method(self.id, has_req)
        # TODO unpack response

    def unhas(self, attribute):
        unhas_req = RequestBuilder.ConceptMethod.Thing.unhas(attribute)
        response = self._tx_service.run_concept_method(self.id, unhas_req)
        # TODO unpack response


class Entity(Thing):
    pass

class Attribute(Thing):

    def value(self):
        value_req = RequestBuilder.ConceptMethod.Attribute.value()
        response = self._tx_service.run_concept_method(self.id, value_req)
        # TODO unpack response

    def owners(self):
        owners_req = RequestBuilder.ConceptMethod.Attribute.owners()
        response = self._tx_service.run_concept_method(self.id, owners_req)
        # TODO unpack response


class Relationship(Thing):

    # NOTE `relation` has replaced `relationship` in ResponseBuilder

    def role_players_map(self):
        role_players_map_req = RequestBuilder.ConceptMethod.Relation.role_players_map()
        response = self._tx_service.run_concept_method(self.id, role_players_map_req)
        # TODO unpack response

    def role_players(self, *roles):
        role_players_req = RequestBuilder.ConceptMethod.Relation.role_players()
        response = self._tx_service.run_concept_method(self.id, role_players_req)
        # TODO unpack response

    def assign(self, role, thing):
        assign_req = RequestBuilder.ConceptMethod.Relation.assign(role, thing)
        response = self._tx_service.run_concept_method(self.id, assign_req)
        # TODO unpack response

    def unassign(self, role, thing):
        unassign_req = RequestBuilder.ConceptMethod.Relation.unassign(role, thing)
        response = self._tx_service.run_concept_method(self.id, unassign_req)
        # TODO unpack response



