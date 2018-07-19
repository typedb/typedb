


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

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    
    def label(self, value=None):
        if value is None:
            # get label
            return None
        else:
            # set label to value
            pass

    def is_implicit(self) -> bool:
        pass

    def sup(self, schema_type=None):
        if schema_type is None:
            # set direct super SchemaConcept of this SchemaConcept
            pass
        else:
            # get direct super schema concept
            pass

    def subs(self):
        pass

    def sups(self):
        pass

class Type(SchemaConcept):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def is_abstract(value: bool = None):
        if value is None:
            # return True/False if the type is set to abstract
            pass
        else:
            # set type to be abstract
            pass

    def playing(self):
        pass

    def plays(self, role):
        pass

    def attributes(self):
        pass

    def instances(self):
        pass

    def keys(self):
        pass

    def key(self, attribute_type):
        pass

    def has(self, attribute_type):
        pass

    def unplay(self, role):
        pass

    def unhas(self, attribute_type):
        pass

    def unkey(self, attribute_type):
        pass



class EntityType(Type):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def create(self):
        pass

class AttributeType(Type):
    
    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

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

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def create(self):
        pass

    def roles(self):
        pass

    def relates(self, role):
        pass

    def unrelate(self, role):
        pass

class Rule(SchemaConcept):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def get_when(self):
        pass

    def get_then(self):
        pass

class Role(SchemaConcept):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def relationships(self):
        pass

    def players(self):
        pass



class Thing(Concept):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

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

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

class Attribute(Thing):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def data_type(self):
        pass

    def value(self):
        pass

    def owners(self):
        pass


class Relationship(Thing):

    def __init__(self, concept_id, base_type, tx_service):
        super().__init__(concept_id, base_type, tx_service)

    def role_players_map(self):
        pass

    def role_players(self, *roles):
        pass

    def assign(self, role, thing):
        pass

    def unassign(self, role, thing):
        pass


