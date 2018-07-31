import unittest
import grakn


class test_PreDbSetup(unittest.TestCase):
    """ Tests Database interactions *before* anything needs to be inserted/created """

    # --- Test grakn client instantiation for one URI ---
    def test_grakn_init_valid(self):
        """ Test valid URI """
        inst = grakn.Grakn('localhost:48555')
        self.assertIsInstance(inst, grakn.Grakn)

    def test_grakn_init_invalid_uri(self):
        """ Test invalid URI """
        # TODO specialize exception
        with self.assertRaises(Exception):
            inst = grakn.Grakn('localhost:1000')
            session = inst.session('testkeyspace')
            session.transaction(grakn.TxType.READ)

    # --- Test grakn session for different keyspaces ---
    def test_grakn_session_valid_keyspace(self):
        """ Test OK uri and keyspace """
        inst = grakn.Grakn('localhost:48555')
        session = inst.session('test')
        self.assertIsInstance(session, grakn.Session)

    def test_grakn_session_invalid_keyspace(self):
        inst = grakn.Grakn('localhost:48555')
        with self.assertRaises(TypeError):
            session = inst.session(123)
            tx = session.transaction(grakn.TxType.READ) # won't fail until opening a transaction
        # TODO specialize exception
        inst2 = grakn.Grakn('localhost:48555')
        with self.assertRaises(Exception):
            session = inst2.session('')
            tx = session.transaction(grakn.TxType.READ) # won't fail until opening a transaction

    def test_grakn_session_close(self):
        inst = grakn.Grakn('localhost:48555')
        session = inst.session('test')
        session.close()
        # TODO specialize exception
        with self.assertRaises(Exception):
            session.transaction(grakn.TxType.READ)

    # --- Test grakn session transactions that are pre-DB setup ---
    def test_grakn_tx_valid_enum(self):
        inst = grakn.Grakn('localhost:48555')
        session = inst.session('test')
        tx = session.transaction(grakn.TxType.READ)
        self.assertIsInstance(tx, grakn.Transaction)

    def test_grakn_tx_invalid_enum(self):
        inst = grakn.Grakn('localhost:48555')
        session = inst.session('test')
        with self.assertRaises(Exception):
            session.transaction('foo')



# shared grakn instances and session for API testing 
inst = grakn.Grakn('localhost:48555')
session = inst.session('testkeyspace')

class test_Base(unittest.TestCase):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        """ Make sure we have some sort of schema and data in DB, only done once """
        super(test_Base, cls).setUpClass()

        # temp tx to set up DB, don't save it
        tx = session.transaction(grakn.TxType.WRITE)
        try:
            # define parentship roles to test agains
            tx.query("define parent sub role; child sub role; mother sub role; son sub role; person sub entity, has age, has gender, plays parent, plays child, plays mother, plays son; age sub attribute datatype long; gender sub attribute datatype string; parentship sub relationship, relates parent, relates child, relates mother, relates son;")
        except Exception as e: # TODO specialize exception
            print(e)
            pass

        answers = list(tx.query("match $x isa person, has age 20; get;"))
        if len(answers) == 0:
            tx.query("insert $x isa person, has age 20;")
        tx.commit()

    def setUp(self):
        self.tx = session.transaction(grakn.TxType.WRITE)

    def tearDown(self):
        self.tx.close()



class test_Transaction(test_Base):
    """ Class for testing transaction methods, eg query, put attribute type... """

    # --- query tests ---
    def test_query_valid_result(self):
        """ Test a valid query """
        answers = self.tx.query("match $x isa person; get;")
        self.assertIsNotNone(answers)


    def test_query_empty_result(self):
        answers = self.tx.query('match $x isa person, has age 9999; get;')
        with self.assertRaises(StopIteration):
            next(answers)


    def test_query_invalid_syntax(self):
        """ Invalid syntax -- expected behavior is an exception & closed transaction """
        # TODO specialize exception
        with self.assertRaises(Exception):
            answers = self.tx.query("match $x bob marley; get")
        with self.assertRaises(Exception):
            # should be closed
            self.tx.query("match $x isa person; get;")
        self.assertTrue(self.tx.is_closed(), msg="Tx is not closed after invalid syntax")


    def test_query_tx_already_closed(self):
        self.tx.close()
        
        # TODO specialize exception
        with self.assertRaises(Exception):
            self.tx.query("match $x isa person; get;")
            
        self.assertTrue(self.tx.is_closed(), msg="Tx is not closed after close()")

    # --- commit tests --- 
    def test_commit_no_error_thrown(self):
        """ TODO double check if we even need this test """
        self.tx.commit()

    def test_commit_normal(self):
        """ Insert, commit, read and check it worked """
        jills_before= len(list(self.tx.query('match $x isa person, has age 10; get;')))
        self.tx.query('insert $x isa person, has age 10;') 
        self.tx.commit()
        # need to open new tx after commit
        tx = session.transaction(grakn.TxType.WRITE)
        answers = tx.query('match $x isa person, has age 10; get;')
        jills_after = len(list(answers))
        self.assertGreater(jills_after, jills_before, msg="Number of entities did not increase after insert and commit")
        

    def test_commit_check_tx_closed(self):
        self.tx.commit()
        self.assertTrue(self.tx.is_closed(), msg="Tx not closed after commit")

    # --- close tests ---

    def test_close_check_closed(self):
        """ Close then confirm closed """
        # attempt to perform a query/put etc
        self.tx.close()
        with self.assertRaises(Exception):
            self.tx.query("match $x isa person; get;")

        self.assertTrue(self.tx.is_closed(), msg="Tx not closed after tx.close()")

    # --- test get concept ---
    def test_get_concept(self):
        """ Test retrieving concept by concept ID """
        people_ids = [answer.get('x').id for answer in self.tx.query('match $x isa person; get;')]
        person_id = people_ids[0]
        
        with self.subTest(i=0):
            # valid ID
            same_person = self.tx.get_concept(person_id)
            self.assertTrue(same_person.is_thing(), msg="Concept retrieved is not a thing")
            self.assertEqual(same_person.id, person_id, msg="Retrieved concept does not have matching ID")
        with self.subTest(i=1):
            # invalid ID
            none_person = self.tx.get_concept('not_an_id')
            self.assertIsNone(none_person, msg="Nonexistant concept ID does not return None")

    # --- test get schema concept ---
    def test_get_schema_concept(self):
        """ Retrieve schema concept (ie a type) by label test """
        with self.subTest(i=0):
            # valid label
            person_schema_type = self.tx.get_schema_concept('person')
            self.assertTrue(person_schema_type.is_schema_concept())
        with self.subTest(i=1):
            # nonexistant label
            not_person_type = self.tx.get_schema_concept('not_a_person')
            self.assertIsNone(not_person_type, msg="Nonexistant label type does not return None")


    # --- test get attributes by value ---
    def test_get_attributes_by_value(self):
        """ Retrieve attribute instances by value """
        with self.subTest(i=0):
            # test retrieving multiple concepts
            firstname_attr_type = self.tx.put_attribute_type("firstname", grakn.DataType.STRING)
            middlename_attr_type = self.tx.put_attribute_type("middlename", grakn.DataType.STRING)
            firstname = firstname_attr_type.create("Billie")
            middlename = middlename_attr_type.create("Billie")
    
            attr_concepts = self.tx.get_attributes_by_value("Billie", grakn.DataType.STRING)
            attr_concepts = list(attr_concepts) # collect iterator
            self.assertEqual(len(attr_concepts), 2, msg="Do not have 2 first name attrs")
    
            ids = [attr.id for attr in attr_concepts]
            self.assertTrue(firstname.id in ids and middlename.id in ids)

        with self.subTest(i=1):
            # test retrieving no concepts
            # because we have no "Jean" attributes
            jean_attrs = list(self.tx.get_attributes_by_value("Jean", grakn.DataType.STRING))
            self.assertEqual(len(jean_attrs), 0)

    def test_put_entity_type(self):
        """ Test get schema entity type by label """
        dog_entity_type = self.tx.put_entity_type('dog')
        self.assertTrue(dog_entity_type.is_schema_concept())
        self.assertTrue(dog_entity_type.is_entity_type())

            
    def test_put_relationship_type(self):
        """ Test putting a schema relationship type """
        marriage_type = self.tx.put_relationship_type('marriage')
        self.assertTrue(marriage_type.is_schema_concept())
        self.assertTrue(marriage_type.is_relationship_type())

    def test_put_attribute_type(self):
        """ Test putting a new attribtue type in schema """
        birthdate = self.tx.put_attribute_type("surname", grakn.DataType.DATE)
        self.assertTrue(birthdate.is_schema_concept())
        self.assertTrue(birthdate.is_attribute_type())

    def test_put_role(self):
        """ Test adding a role """
        role = self.tx.put_role("spouse")
        self.assertTrue(role.is_role(), msg="created role type is not Role")
        self.assertEqual(role.base_type, "ROLE", msg="Role base_type is not ROLE")

    def test_put_rule(self):
        """ Test adding a rule for genderized parentship"""

        # create a role which creates a trivial "ancestor" relationship
        label = "genderizedparentship"
        when = "{(parent: $p, child: $c) isa parentship; $p has gender 'female'; $c has gender 'male';}"
        then = "{(mother: $p, son: $c) isa parentship;}"

        rule = self.tx.put_rule(label, when, then)
        self.assertTrue(rule.is_rule())
        self.assertEqual(rule.label(), label)
        

            

class test_Type(test_Base):
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
            update_person_plays = person_schema_type.playing()
            labels = [role.labels() for role in updated_person_plays]
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


class test_EntityType(test_Base):

    def test_create(self):
        person_type = self.tx.get_schema_concept("person")
        person = person_type.create()
        self.assertTrue(person.is_entity())


class test_AttributeType(test_Base):

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


class test_RelationshipType(test_Base):

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


class test_Rule(test_Base):

    def test_when_then(self):
        """ Test get valid  when/then """
        label = "genderizedparentship"
        when = "{(parent: $p, child: $c) isa parentship; $c has gender \"male\"; $p has gender \"female\";}"
        then = "{(mother: $p, son: $c) isa parentship;}"
        rule = self.tx.put_rule(label, when, then)

        self.assertEqual(rule.get_when(), when)
        self.assertEqual(rule.get_then(), then)

    def test_none_when_then(self):
        """ Test get when/then for nonexistant rule """
        rule = self.tx.get_schema_concept('rule')
        self.assertIsNone(rule.get_when())
        self.assertIsNone(rule.get_then())


class test_Role(test_Base):

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


class test_Thing(test_Base):

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


class test_Attribute(test_Base):

    def test_value(self):
        """ Get attribute value """
        double_attr_type = self.tx.put_attribute_type("length", grakn.DataType.DOUBLE)
        double = double_attr_type.create(43.1)
        self.assertEqual(double.value(), 43.1)

    def test_date_value(self):
        # TODO
        print(" ------ TODO ------ ")
        pass

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


class test_Relationship(test_Base):

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
        print("------ ")
        print([x.id for x in role_players])
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
