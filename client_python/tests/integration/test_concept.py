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

import unittest
import grakn
import datetime
from grakn.exception.GraknError import GraknError


from tests.integration.base import test_Base

inst = grakn.Grakn("localhost:48555")
session = inst.session("testkeyspace")

class test_concept_Base(test_Base):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        """ Make sure we have some sort of schema and data in DB, only done once """
        super(test_concept_Base, cls).setUpClass()

        # temp tx to set up DB, don"t save it
        tx = session.transaction(grakn.TxType.WRITE)
        try:
            # define parentship roles to test agains
            tx.query("define parent sub role; child sub role; mother sub role; son sub role; person sub entity, has age, has gender, plays parent, plays child, plays mother, plays son; age sub attribute datatype long; gender sub attribute datatype string; parentship sub relationship, relates parent, relates child, relates mother, relates son;")
        except GraknError as ce: 
            print(ce)

        answers = list(tx.query("match $x isa person, has age 20; get;"))
        if len(answers) == 0:
            tx.query("insert $x isa person, has age 20;")
        tx.commit()

    def setUp(self):
        self.tx = session.transaction(grakn.TxType.WRITE)

    def tearDown(self):
        self.tx.close()



class test_Concept(test_concept_Base):
    """ Test methods available on all Concepts """

    def test_delete_schema_types(self):
        car_type = self.tx.put_entity_type("car")
        
        schema_concept = self.tx.get_schema_concept("car")
        self.assertTrue(schema_concept.is_schema_concept())
        schema_concept.delete()
        none_schema_car = self.tx.get_schema_concept("car")
        self.assertIsNone(none_schema_car, msg="Deletion of car schema type failed")

    def test_delete_instance(self):
        car_type = self.tx.put_entity_type("car")
        car = car_type.create()

        car.delete()
        none_car = self.tx.get_concept(car.id)
        self.assertIsNone(none_car, msg="Deletion of car instance failed")

    def test_re_delete_instance(self):
        car_type = self.tx.put_entity_type("car")
        car = car_type.create()
        
        car.delete()
        none_car = self.tx.get_concept(car.id)
        self.assertIsNone(none_car)

        with self.assertRaises(GraknError):
            car.delete()

    
    def test_is_each_schema_type(self):
        car_type = self.tx.put_entity_type("car")
        car = car_type.create()
        self.assertTrue(car.is_entity())
        self.assertFalse(car.is_attribute())
        self.assertFalse(car.is_relationship())

        rel_type = self.tx.put_relationship_type("owner")
        owner = rel_type.create()
        self.assertFalse(owner.is_entity())
        self.assertFalse(owner.is_attribute())
        self.assertTrue(owner.is_relationship())

        attr_type = self.tx.put_attribute_type("age", grakn.DataType.LONG)
        age = attr_type.create(50)
        self.assertFalse(age.is_entity())
        self.assertTrue(age.is_attribute())
        self.assertFalse(age.is_relationship())

class test_SchemaConcept(test_concept_Base):
    """ Test methods available on all SchemaConcepts """
    
    def test_set_label(self):
        """ Get and set labels """
        with self.subTest(i=0):
            # get label
            car_schema_type = self.tx.put_entity_type("car")
            car_type = self.tx.get_schema_concept("car")
            self.assertEqual(car_type.label(), "car")

        with self.subTest(i=1):
            # set label
            car_type = self.tx.get_schema_concept("car")
            car_type.label("vehicle")
            vehicle_type = self.tx.get_schema_concept("vehicle")
            self.assertEqual(vehicle_type.label(), "vehicle")

        with self.subTest(i=2):
            bike_type = self.tx.get_schema_concept("bike")
            with self.assertRaises(AttributeError):
                bike_type.label("")
            with self.assertRaises(AttributeError):
                bike_type.label(100)
            self.assertIsNone(bike_type)

    def test_is_implicit(self):
        """ Test implicit schema concepts """
        person = self.tx.get_schema_concept("person")
        self.assertFalse(person.is_implicit())
        implicit_concept = self.tx.get_schema_concept("@has-age")
        self.assertTrue(implicit_concept.is_implicit())

    def test_get_sups(self):
        """ Test get super types of a schema concept -- recall a type is supertype of itself always """
        person = self.tx.get_schema_concept("person")
        sups = list(person.sups())
        self.assertEqual(len(sups), 2, msg="person does not have 2 sups")
        sup_labels = [concept.label() for concept in sups]
        self.assertTrue("person" in sup_labels and "entity" in sup_labels)

        # check supertype of toplevel schema concepts
        schema_entity = self.tx.get_schema_concept("entity")
        thing_type = schema_entity.sup() # this is Thing
        self.assertEqual(thing_type.base_type, "META_TYPE")
        thing_sup = thing_type.sup()
        self.assertIsNone(thing_sup)

    def test_set_sups(self):
        """ Test setting super type of a schema concept """
        human_schema_concept = self.tx.put_entity_type("human")
        male_schema_concept = self.tx.put_entity_type("male")
        human_sup = human_schema_concept.sup()
        self.assertEqual(human_sup.base_type, "ENTITY_TYPE")

        male_schema_concept.sup(human_schema_concept)
        sup = male_schema_concept.sup()
        self.assertEqual(sup.label(), "human")

    def test_get_subs(self):
        """ Test get sub types of schema concept -- recall a type is a subtype of itself always """
        entity = self.tx.get_schema_concept("entity")
        subs = list(entity.subs())
        self.assertEqual(len(subs), 2, msg="entity does not have 2 subs")
        subs_labels = [sub.label() for sub in subs]
        self.assertTrue('entity' in subs_labels and 'person' in subs_labels)




class test_Type(test_concept_Base):
    """ Tests concept API of things common to Type objects """

    def test_is_abstract(self):
        """ Tests get/set of is_abstract on types """
        dog_type = self.tx.put_entity_type("dog")
        with self.subTest(i=0):
            abstract = dog_type.is_abstract()
            self.assertFalse(abstract)
        with self.subTest(i=1):
            dog_type.is_abstract(True)
            abstract = dog_type.is_abstract() #re-retrieve from server
            self.assertTrue(abstract)
        with self.subTest(i=2):
            dog_type.is_abstract(False)
            abstract = dog_type.is_abstract()
            self.assertFalse(abstract)

    def test_plays_methods(self):
        """ Test get/set/delete plays ie. roles """
        father = self.tx.put_role("father")
        with self.subTest(i=0):
            person_schema_type = self.tx.get_schema_concept("person")
            person_plays = list(person_schema_type.playing())
            # by default, they play 4 explicit roles and 2 @has-... roles 
            self.assertEqual(len(person_plays), 6)
        with self.subTest(i=1):
            person_schema_type.plays(father)
            updated_person_plays = person_schema_type.playing()
            labels = [role.label() for role in updated_person_plays]
            self.assertEqual(len(labels), 7)
            self.assertTrue("father" in labels)
        with self.subTest(i=2): 
            # remove role/plays from person
            person_schema_type.unplay(father)
            updated_person_plays = person_schema_type.playing()
            labels = [role.label() for role in updated_person_plays]
            self.assertEqual(len(labels), 6)
            self.assertFalse("father" in labels)

    def test_attributes_methods(self):
        """ Test get/set/delete attributes """
        person = self.tx.get_schema_concept("person")
        haircolor_attr = self.tx.put_attribute_type("haircolor", grakn.DataType.STRING)
        with self.subTest(i=0):
            # get attrs
            current_attrs = person.attributes()
            labels = [attr.label() for attr in current_attrs]
            self.assertEqual(len(labels), 2) # has age, gender to start with
        with self.subTest(i=1):
            # add an attr
            person.has(haircolor_attr)
            new_attrs = person.attributes()
            new_labels = [attr.label() for attr in new_attrs]
            self.assertEqual(len(new_labels), 3)
            self.assertTrue('haircolor' in new_labels)
        with self.subTest(i=2):
            # delete an attrs
            person.unhas(haircolor_attr)
            attrs_fewer = person.attributes()
            labels_fewer = [attr.label() for attr in attrs_fewer]
            self.assertEqual(len(labels_fewer), 2)
            self.assertFalse('haircolor' in labels_fewer)

    def test_instances(self):
        """ Test retrieving instances of a type """
        person = self.tx.get_schema_concept("person")
        people = list(person.instances())
        person_inst = person.create()
        people_more = list(person.instances())
        self.assertEqual(len(people_more) - len(people), 1)

    def test_key(self):
        """ Test get/set/delete key on Type """
        person_type = self.tx.get_schema_concept("person")
        name_attr_type = self.tx.put_attribute_type('name', grakn.DataType.STRING)

        with self.subTest(i=0):
            # check current keys
            keys = list(person_type.keys())
            self.assertEqual(len(keys), 0, "Person has more than 0 keys already")
        with self.subTest(i=1):
            # set a key
            person_type.key(name_attr_type)
            keys = list(person_type.keys())
            self.assertEqual(len(keys), 1)
            self.assertEqual(keys[0].base_type, "ATTRIBUTE_TYPE")
            self.assertEqual(keys[0].label(), 'name')
        with self.subTest(i=2):
            # remove a key
            person_type.unkey(name_attr_type)
            keys = list(person_type.keys())
            self.assertEqual(len(keys), 0)


class test_EntityType(test_concept_Base):

    def test_create(self):
        person_type = self.tx.get_schema_concept("person")
        person = person_type.create()
        self.assertTrue(person.is_entity())


class test_AttributeType(test_concept_Base):

    def test_create(self):
        str_attr_type = self.tx.put_attribute_type("firstname", grakn.DataType.STRING)
        john = str_attr_type.create("john")
        self.assertTrue(john.is_attribute())
        self.assertEqual(john.value(), "john")

        bool_attr_type = self.tx.put_attribute_type("employed", grakn.DataType.BOOLEAN)
        employed = bool_attr_type.create(True)
        self.assertEqual(employed.value(), True)

        double_attr_type = self.tx.put_attribute_type("length", grakn.DataType.DOUBLE)
        one = double_attr_type.create(1.0)
        self.assertEqual(one.value(), 1.0)

    def test_data_type(self):
        str_attr_type = self.tx.put_attribute_type("firstname", grakn.DataType.STRING)
        self.assertEqual(str_attr_type.data_type(), grakn.DataType.STRING)

        bool_attr_type = self.tx.put_attribute_type("employed", grakn.DataType.BOOLEAN)
        self.assertEqual(bool_attr_type.data_type(), grakn.DataType.BOOLEAN)

        double_attr_type = self.tx.put_attribute_type("length", grakn.DataType.DOUBLE)
        self.assertEqual(double_attr_type.data_type(), grakn.DataType.DOUBLE)

        long_attr_type = self.tx.put_attribute_type("randomint", grakn.DataType.LONG)
        self.assertEqual(long_attr_type.data_type(), grakn.DataType.LONG)

    def test_attribute(self):
        """ Test retrieve attribute instances """

        name = self.tx.put_attribute_type("name", grakn.DataType.STRING)
        john = name.create("john")
        
        with self.subTest(i=0):
            # retrieve existing attr inst
            retrieved_john = name.attribute("john")
            self.assertEqual(retrieved_john.value(), john.value())
            self.assertTrue(retrieved_john.is_attribute())
        with self.subTest(i=1):
            # retrieve nonexistant attr inst
            retrieved_none = name.attribute("nobody")
            self.assertIsNone(retrieved_none)

    def test_regex(self):
        """ Test get/set regex """
        attr_type = self.tx.put_attribute_type("dogbadness", grakn.DataType.STRING)

        empty_regex = attr_type.regex()
        self.assertEqual(len(empty_regex), 0, msg="Unset regex does not have length 0")

        attr_type.regex("(good|bad)-dog")
        regex = attr_type.regex()
        self.assertEqual(regex, "(good|bad)-dog")


class test_RelationshipType(test_concept_Base):

    def test_create(self):
        rel_type = self.tx.put_relationship_type("owner")
        rel = rel_type.create()
        self.assertTrue(rel.is_relationship())
        self.assertTrue(rel_type.is_relationship_type())

    def test_relates(self):
        """ Test get/relate/unrelate roles for a relationship type """
        ownership = self.tx.put_relationship_type("ownership")
        role_owner = self.tx.put_role("owner")
        role_owned = self.tx.put_role("owned")

        with self.subTest(i=0):
            # currently no roles in the new relationship
            roles = list(ownership.roles())
            self.assertEqual(len(roles), 0)
        with self.subTest(i=1):
            # set roles in relationship
            ownership.relates(role_owner)
            ownership.relates(role_owned)
            roles = list(ownership.roles())
            self.assertEqual(len(roles), 2)
        with self.subTest(i=2):
            # unrelate a role
            ownership.unrelate(role_owned)
            roles = list(ownership.roles())
            self.assertEqual(len(roles), 1)
            self.assertEqual(roles[0].base_type, "ROLE")


class test_Rule(test_concept_Base):

    def test_when_then(self):
        """ Test get valid  when/then """
        label = "genderizedparentship"
        when = "{(parent: $p, child: $c) isa parentship; $c has gender \"male\"; $p has gender \"female\";}"
        then = "{(mother: $p, son: $c) isa parentship;}"
        rule = self.tx.put_rule(label, when, then)

        self.assertEqual(rule.get_when(), when)
        self.assertEqual(rule.get_then(), then)

    def test_none_when_then(self):
        """ Test get when/then for rule with null when/then """
        rule = self.tx.get_schema_concept('rule')
        self.assertIsNone(rule.get_when())
        self.assertIsNone(rule.get_then())


class test_Role(test_concept_Base):

    def test_relationships(self):
        """ Test retrieving relationships of a role """
        # parent role, parentship already exist
        result = self.tx.query("match $x label parent; get;").collect_concepts()
        parent_role = result[0]
        self.assertEqual(parent_role.base_type, "ROLE")

        relationships = list(parent_role.relationships())
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0].base_type, "RELATIONSHIP_TYPE")
        self.assertEqual(relationships[0].label(), "parentship")

    def test_players(self):
        """ Test retrieving entity types playing this role """
        result = self.tx.query("match $x label parent; get;").collect_concepts()
        parent_role = result[0]
        self.assertEqual(parent_role.base_type, "ROLE")

        entity_types = list(parent_role.players())
        self.assertEqual(len(entity_types), 1)
        self.assertEqual(entity_types[0].base_type, "ENTITY_TYPE")
        self.assertEqual(entity_types[0].label(), "person")


class test_Thing(test_concept_Base):

    def test_is_inferred(self):
        person_type = self.tx.get_schema_concept("person")
        person = person_type.create()
        self.assertFalse(person.is_inferred())

    def test_type(self):
        person_type = self.tx.get_schema_concept("person")
        person = person_type.create()
        p_type = person.type()
        self.assertEqual(p_type.id, person_type.id) # same schema concept 
        self.assertTrue(p_type.is_type())

    def test_relationships(self):
        """ Test retrieve relationships narrowed optionally by roles """
        # create a first relationship
        sibling_type = self.tx.put_relationship_type('sibling')
        brother_role = self.tx.put_role("brother")
        sibling_type.relates(brother_role)
        person = self.tx.get_schema_concept("person")

        # create a second relationship
        ownership_type = self.tx.put_relationship_type("ownership")
        owner_role = self.tx.put_role("owner")
        ownership_type.relates(owner_role)
        person.plays(owner_role)
       
        # connect entities/relationship instances
        sibling = sibling_type.create()
        ownership = ownership_type.create()
        son = person.create()
        sibling.assign(brother_role, son) # assign son to sibling rel
        ownership.assign(owner_role, son) # attach son to owner rel 

        # retrieve all relationships
        rels = list(son.relationships()) 
        self.assertEqual(len(rels), 2)
        rel_ids = [rel.id for rel in rels]
        self.assertTrue(sibling.id in rel_ids and ownership.id in rel_ids)
        

        # retrieve filtered by only the owner role
        filtered_rels = list(son.relationships(owner_role))
        self.assertEqual(len(filtered_rels), 1)
        self.assertEqual(filtered_rels[0].id, ownership.id)
    
    def test_roles(self):
        # create a relationship
        ownership_type = self.tx.put_relationship_type("ownership")
        owner_role = self.tx.put_role("owner")
        ownership_type.relates(owner_role)
        person_type = self.tx.get_schema_concept("person")
        person_type.plays(owner_role)
       
        # connect entities/relationship instances
        ownership = ownership_type.create()
        person = person_type.create()
        ownership.assign(owner_role, person) # attach son to owner rel 
       
        roles = list(person.roles())
        self.assertEqual(len(roles), 1)
        self.assertEqual(roles[0].id, owner_role.id)


    def test_has_unhas_attributes(self):
        """ Test has/unhas/get attributes """
        person_type = self.tx.get_schema_concept("person")
        name_attr_type = self.tx.put_attribute_type("name", grakn.DataType.STRING)
        person_type.has(name_attr_type)
        person = person_type.create()
        attr_john = name_attr_type.create("john")
        person.has(attr_john)

        attrs = list(person.attributes())
        self.assertEqual(len(attrs), 1)
        self.assertEqual(attrs[0].id, attr_john.id)

        person.unhas(attr_john)
        empty_attrs = list(person.attributes())
        self.assertEqual(len(empty_attrs), 0)

    def test_attributes(self):
        """ Test retrieve attrs optionally narrowed by types """
        person_type = self.tx.get_schema_concept("person")
        name_attr = self.tx.put_attribute_type("name", grakn.DataType.STRING)
        foo_attr = self.tx.put_attribute_type("foo", grakn.DataType.BOOLEAN)
        bar_attr = self.tx.put_attribute_type("bar", grakn.DataType.LONG)
        
        person_type.has(name_attr)
        person_type.has(foo_attr)

        person = person_type.create()
        name = name_attr.create("john")
        foo = foo_attr.create(False)
        person.has(name)
        person.has(foo)
        
        attrs = list(person.attributes())
        self.assertEqual(len(attrs), 2)
        for attr in attrs:
            self.assertTrue(attr.is_attribute())

        #filtered attrs
        attrs = list(person.attributes(name_attr))
        self.assertEqual(len(attrs), 1)
        self.assertTrue(attrs[0].is_attribute())
        self.assertEqual(attrs[0].id, name.id)
        attrs = list(person.attributes(name_attr, foo_attr))
        self.assertEqual(len(attrs), 2)

        #nonexistant filtering
        attrs = list(person.attributes(bar_attr)) # not attached
        self.assertEqual(len(attrs), 0)

    def test_keys(self):
        """ Test retrieving keys optionally filtered by attribute types """
        person_type = self.tx.get_schema_concept("person")
        name_type = self.tx.put_attribute_type("name", grakn.DataType.STRING)
        surname_type = self.tx.put_attribute_type("surname", grakn.DataType.STRING)
        person_type.key(name_type)
        person_type.has(surname_type)
        
        name = name_type.create("john")
        surname = surname_type.create("lennon")
        person = person_type.create()
        person.has(name)
        person.has(surname)

        keys = list(person.keys())
        self.assertEqual(len(keys), 1)
        self.assertEqual(keys[0].id, name.id)

        filtered_keys = list(person.keys(name_type, surname_type))
        self.assertEqual(len(filtered_keys), 1)
        self.assertEqual(filtered_keys[0].id, name.id)
        
        empty_keys = list(person.keys(surname_type))
        self.assertEqual(len(empty_keys), 0)


class test_Attribute(test_concept_Base):

    def test_value(self):
        """ Get attribute value """
        double_attr_type = self.tx.put_attribute_type("length", grakn.DataType.DOUBLE)
        double = double_attr_type.create(43.1)
        self.assertEqual(double.value(), 43.1)

    def test_get_date_value(self):
        date_type = self.tx.put_attribute_type("birthdate", grakn.DataType.DATE)
        person_type = self.tx.get_schema_concept("person")
        person_type.has(date_type)
        concepts = self.tx.query("insert $x isa person, has birthdate 2018-08-06;").collect_concepts()
        person = concepts[0]
        attrs_iter = person.attributes()
        for attr_concept in attrs_iter:
            # pick out the birthdate
            if attr_concept.type().label() == "birthdate":
                date = attr_concept.value()
                self.assertIsInstance(date, datetime.datetime)
                self.assertEqual(date.year, 2018)
                self.assertEqual(date.month, 8)
                self.assertEqual(date.day, 6)
                return


    def test_set_date_value(self):
        date_type = self.tx.put_attribute_type("birthdate", grakn.DataType.DATE)
        test_date = datetime.datetime(year=2018, month=6, day=6)
        date_attr_inst = date_type.create(test_date)
        value = date_attr_inst.value() # retrieve from server
        self.assertIsInstance(value, datetime.datetime)
        self.assertEqual(value.timestamp(), test_date.timestamp())

        

    def test_owners(self):
        """ Test retrieving entities that have an attribute """
        person_type = self.tx.get_schema_concept("person")
        animal_type = self.tx.put_entity_type("animal")
        name_type = self.tx.put_attribute_type("name", grakn.DataType.STRING)
        person_type.has(name_type)
        animal_type.has(name_type)

        person = person_type.create()
        animal = animal_type.create()
        john = name_type.create("john")

        person.has(john)
        animal.has(john)

        owners = list(john.owners())
        self.assertEqual(len(owners), 2)
        labels = [x.id for x in owners]
        self.assertTrue(person.id in labels and animal.id in labels)


class test_Relationship(test_concept_Base):

    def test_role_players_2_roles_1_player(self):
        """ Test role_players_map and role_players with 2 roles and 1 player each """
        parentship_type = self.tx.get_schema_concept("parentship")
        person_type = self.tx.get_schema_concept("person")
        parent_role = self.tx.get_schema_concept("parent")
        child_role = self.tx.get_schema_concept("child")

        parent = person_type.create()
        child = person_type.create()
        parentship = parentship_type.create()

        parentship.assign(parent_role, parent)
        parentship.assign(child_role, child)

        role_players_map = parentship.role_players_map()
        self.assertEqual(len(role_players_map.keys()), 2)
        for role in role_players_map:
            players_set = role_players_map[role]
            self.assertEqual(len(players_set), 1)
            self.assertTrue(role.is_role())

        role_players = list(parentship.role_players())
        self.assertEqual(len(role_players), 2)

    def test_role_players_1_role_2_players(self):

        parentship_type = self.tx.get_schema_concept("parentship")
        person_type = self.tx.get_schema_concept("person")
        parent_role = self.tx.get_schema_concept("parent")

        parent = person_type.create()
        another_parent = person_type.create()
        parentship = parentship_type.create()        
        
        parentship.assign(parent_role, parent)
        parentship.assign(parent_role, another_parent)
        
        role_players_map = parentship.role_players_map()
        self.assertEqual(len(role_players_map.keys()), 1)
        for role in role_players_map:
            players_set = role_players_map[role]
            self.assertEqual(len(players_set), 2)
            self.assertTrue(role.is_role())

        role_players = list(parentship.role_players())
        self.assertEqual(len(role_players), 2)

    def test_role_players_2_roles_same_player(self):
        parentship_type = self.tx.get_schema_concept("parentship")
        person_type = self.tx.get_schema_concept("person")
        parent_role = self.tx.get_schema_concept("parent")
        child_role = self.tx.get_schema_concept("child")

        self_parent = person_type.create()
        parentship = parentship_type.create()
        
        parentship.assign(parent_role, self_parent)
        parentship.assign(child_role, self_parent)
        
        role_players_map = parentship.role_players_map()
        self.assertEqual(len(role_players_map.keys()), 2)
        for role in role_players_map:
            players_set = role_players_map[role]
            self.assertEqual(len(players_set), 1)
            self.assertTrue(role.is_role())

        role_players = list(parentship.role_players())
        self.assertEqual(len(role_players), 1)
        self.assertTrue(role_players[0].is_thing())

    def test_assign_unassign(self):
        parentship_type = self.tx.get_schema_concept("parentship")
        person_type = self.tx.get_schema_concept("person")
        parent_role = self.tx.get_schema_concept("parent")

        person = person_type.create()
        parentship = parentship_type.create()

        empty_role_players = list(parentship.role_players())
        self.assertEqual(len(empty_role_players), 0)
        
        parentship.assign(parent_role, person)
        role_players = list(parentship.role_players())
        self.assertEqual(len(role_players), 1)
        self.assertEqual(role_players[0].id, person.id)

        parentship.unassign(parent_role, person)
        post_remove_role_players = list(parentship.role_players())
        self.assertEqual(len(post_remove_role_players), 0)

    
    def test_role_players_filtered_by_role(self):
        parentship_type = self.tx.get_schema_concept("parentship")
        person_type = self.tx.get_schema_concept("person")
        parent_role = self.tx.get_schema_concept("parent")
        child_role = self.tx.get_schema_concept("child")

        parent = person_type.create()
        child = person_type.create()
        parentship = parentship_type.create()
        parentship.assign(parent_role, parent)
        parentship.assign(child_role, child)
    
        # no filter
        role_players = list(parentship.role_players())
        self.assertEqual(len(role_players), 2)
        # single filter
        filtered_role_players = list(parentship.role_players(child_role))
        self.assertEqual(len(filtered_role_players), 1)
        self.assertEqual(filtered_role_players[0].id, child.id)

        # allow both
        double_filter_role_players = list(parentship.role_players(child_role, parent_role))
        self.assertEqual(len(double_filter_role_players), 2)

        

    
if __name__ == "__main__":
    unittest.main()

    
