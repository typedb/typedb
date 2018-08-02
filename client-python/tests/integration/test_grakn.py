import unittest
import grakn
from grakn.exception.ClientError import ClientError


class test_PreDbSetup(unittest.TestCase):
    """ Tests Database interactions *before* anything needs to be inserted/created """

    # --- Test grakn client instantiation for one URI ---
    def test_grakn_init_valid(self):
        """ Test valid URI """
        inst = grakn.Grakn('localhost:48555')
        self.assertIsInstance(inst, grakn.Grakn)

    def test_grakn_init_invalid_uri(self):
        """ Test invalid URI """
        with self.assertRaises(ClientError):
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
        inst2 = grakn.Grakn('localhost:48555')
        with self.assertRaises(ClientError):
            session = inst2.session('')
            tx = session.transaction(grakn.TxType.READ) # won't fail until opening a transaction

    def test_grakn_session_close(self):
        inst = grakn.Grakn('localhost:48555')
        session = inst.session('test')
        session.close()
        with self.assertRaises(ClientError):
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
        except ClientError as ce:
            print(ce)

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
        with self.assertRaises(ClientError):
            answers = self.tx.query("match $x bob marley; get")
        with self.assertRaises(ClientError):
            # should be closed
            self.tx.query("match $x isa person; get;")
        self.assertTrue(self.tx.is_closed(), msg="Tx is not closed after invalid syntax")


    def test_query_tx_already_closed(self):
        self.tx.close()
        with self.assertRaises(ClientError):
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

    # --- test schema modification ---
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
        
if __name__ == "__main__":
    unittest.main()
