from ..util.RequestBuilder import RequestBuilder


class Concept(object):

    def __init__(self, concept_id, base_type, tx_service):
        self.id = concept_id
        self.base_type = base_type
        self._tx_service = tx_service


    def delete(self):
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

    def label(self, value=None):
        if value is None:
            get_label_req = RequestBuilder.ConceptMethod.SchemaConcept.get_label()
            response = self._tx_service.run_concept_method(self.id, get_label_req)
            # TODO unpack response

        else:
            set_label_req = RequestBuilder.ConceptMethod.SchemaConcept.set_label(value)
            response = self._tx_service.run_concept_method(self.id, set_label_req)
            # TODO unpack response

    def is_implicit(self) -> bool:
        is_implicit_req = RequestBuilder.ConceptMethod.SchemaConcept.is_implicit()
        response = self._tx_service.run_concept_method(self.id, is_implicit_req)
        # TODO unpack response

    def sup(self, super_concept=None):
        if super_concept is None:
            # get direct super schema concept
            get_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.get_sup()
            response = self._tx_service.run_concept_method(self.id, get_sup_req)
            # TODO unpack response 
        else:
            # set direct super SchemaConcept of this SchemaConcept
            set_sup_req = RequestBuilder.ConceptMethod.SchemaConcept.set_sup(super_concept)
            response = self._tx_service.run_concept_method(self.id, set_sup_req)
            # TODO unpack response

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
        pass

class AttributeType(Type):
    
    def create(self, value):
        pass

    def attribute(self, value):
        pass

    def data_type(self):
        pass

    def regex(self, pattern: str = None):
        if pattern is None:
            # retrieve the regex or None if no regex is set
            pass
        else:
            # set regex
            pass


class RelationshipType(Type):

    def create(self):
        pass

    def roles(self):
        pass

    def relates(self, role):
        pass

    def unrelate(self, role):
        pass

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
        pass

    def type(self):
        pass

    def relationships(self, *roles):
        pass

    def attributes(self, *attribute_types):
        pass

    def plays(self):
        pass

    def keys(self, *attribute_types):
        pass

    def has(self, attribute):
        pass

    def unhas(self, attribute):
        pass


class Entity(Thing):
    pass

class Attribute(Thing):

    def data_type(self):
        pass

    def value(self):
        pass

    def owners(self):
        pass


class Relationship(Thing):

    def role_players_map(self):
        pass

    def role_players(self, *roles):
        pass

    def assign(self, role, thing):
        pass

    def unassign(self, role, thing):
        pass


