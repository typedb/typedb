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
from grakn.exception.GraknError import GraknError
from grakn.service.Session.util.ResponseReader import Answer, Value, ConceptList, ConceptSet, ConceptSetMeasure, AnswerGroup

from tests.integration.base import test_Base


class test_grakn_PreDbSetup(test_Base):
    """ Tests Database interactions *before* anything needs to be inserted/created """

    # --- Test grakn client instantiation for one URI ---
    def test_grakn_init_valid(self):
        """ Test valid URI """
        a_inst = grakn.Grakn('localhost:48555')
        self.assertIsInstance(inst, grakn.Grakn)

    def test_grakn_init_invalid_uri(self):
        """ Test invalid URI """
        with self.assertRaises(GraknError):
            a_inst = grakn.Grakn('localhost:1000')
            a_session = a_inst.session('testkeyspace')
            a_session.transaction(grakn.TxType.READ)

        with self.assertRaises(GraknError):
            a_inst = grakn.Grakn('localhost:1000')
            with a_inst.session("test") as s:
                with s.transaction(grakn.TxType.READ) as tx:
                    pass
            


    # --- Test grakn session for different keyspaces ---
    def test_grakn_session_valid_keyspace(self):
        """ Test OK uri and keyspace """
        a_inst = grakn.Grakn('localhost:48555')
        a_session = a_inst.session('test')
        self.assertIsInstance(a_session, grakn.Session)

        # test the `with` statement
        with a_inst.session('test') as session:
            self.assertIsInstance(session, grakn.Session)

    def test_grakn_session_invalid_keyspace(self):
        inst = grakn.Grakn('localhost:48555')
        with self.assertRaises(TypeError):
            a_session = inst.session(123)
            tx = a_session.transaction(grakn.TxType.READ) # won't fail until opening a transaction
        inst2 = grakn.Grakn('localhost:48555')
        with self.assertRaises(GraknError):
            a_session = inst2.session('')
            tx = a_session.transaction(grakn.TxType.READ) # won't fail until opening a transaction

    def test_grakn_session_close(self):
        inst = grakn.Grakn('localhost:48555')
        a_session = inst.session('test')
        a_session.close()
        with self.assertRaises(GraknError):
            a_session.transaction(grakn.TxType.READ)

    # --- Test grakn session transactions that are pre-DB setup ---
    def test_grakn_tx_valid_enum(self):
        inst = grakn.Grakn('localhost:48555')
        a_session = inst.session('test')
        tx = a_session.transaction(grakn.TxType.READ)
        self.assertIsInstance(tx, grakn.Transaction)

    def test_grakn_tx_invalid_enum(self):
        inst = grakn.Grakn('localhost:48555')
        a_session = inst.session('test')
        with self.assertRaises(Exception):
            a_session.transaction('foo')



inst = grakn.Grakn('localhost:48555')
session = inst.session('testkeyspace')

class test_grakn_Base(test_Base):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        """ Make sure we have some sort of schema and data in DB, only done once """
        super(test_grakn_Base, cls).setUpClass()
        # shared grakn instances and session for API testing 

        # temp tx to set up DB, don't save it
        with session.transaction(grakn.TxType.WRITE) as tx:
            tx = session.transaction(grakn.TxType.WRITE)
            try:
                # define parentship roles to test agains
                tx.query("define "
                         "parent sub role; "
                         "child sub role; "
                         "mother sub role; "
                         "son sub role; "
                         "person sub entity, has age, has gender, plays parent, plays child, plays mother, plays son; "
                         "age sub attribute, datatype long; "
                         "gender sub attribute, datatype string; "
                         "parentship sub relationship, relates parent, relates child, relates mother, relates son;")
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



class test_Transaction(test_grakn_Base):
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
        with self.assertRaises(GraknError):
            answers = self.tx.query("match $x bob marley; get")
        with self.assertRaises(GraknError):
            # should be closed
            self.tx.query("match $x isa person; get;")
        self.assertTrue(self.tx.is_closed(), msg="Tx is not closed after invalid syntax")


    def test_query_tx_already_closed(self):
        self.tx.close()
        with self.assertRaises(GraknError) :
            self.tx.query("match $x isa person; get;")
            
        self.assertTrue(self.tx.is_closed(), msg="Tx is not closed after close()")

    def test_no_metatype_duplicates(self):
        concepts = self.tx.query("match $x sub entity; get;").collect_concepts()
        self.assertEqual(len(concepts), 2) # entity and person
        id_set = set(concepts)
        self.assertEqual(len(id_set), 2) # entity and person, not the same

    def test_compute_count_empty_graph_anwer_Value(self):
        self.tx.put_entity_type("foo")
        result = self.tx.query("compute count in foo;")
        answer = next(result)
        self.assertIsInstance(answer, Value) # specific type of Answer
        self.assertEqual(answer.number(), 0)

    def test_aggr_count_empty_graph_anwer_Value(self):
        result = self.tx.query("match $x sub entity; get $x; count;")
        answer = next(result)
        self.assertIsInstance(answer, Value)
        self.assertEqual(answer.number(), 2)


    @staticmethod
    def _build_parentship(tx):
        """ Helper to set up some state to test answers in a tx/keyspace """
        parentship_type = tx.put_relationship_type("parentship")
        parentship = parentship_type.create()
        parent_role = tx.put_role("parent")
        child_role = tx.put_role("child")
        parentship_type.relates(parent_role)
        parentship_type.relates(child_role)
        person_type = tx.put_entity_type("person")
        person_type.plays(parent_role)
        person_type.plays(child_role)
        parent = person_type.create()
        child = person_type.create()
        parentship.assign(child_role, child)
        parentship.assign(parent_role, parent)
        tx.commit() # closes the tx
        return {'child': child.id, 'parent': parent.id, 'parentship': parentship.id}

    def test_shortest_path_answer_ConceptList(self):
        """ Test shortest path which returns a ConceptList """
        local_session = inst.session("shortestpath")
        tx = local_session.transaction(grakn.TxType.WRITE)
        parentship_map = test_Transaction._build_parentship(tx) # this closes the tx
        tx = local_session.transaction(grakn.TxType.WRITE)
        result = tx.query('compute path from "{0}", to "{1}";'.format(parentship_map['parent'], parentship_map['child']))
        answer = next(result)
        self.assertIsInstance(answer, ConceptList)
        self.assertEqual(len(answer.list()), 3)
        self.assertTrue(parentship_map['parent'] in answer.list())
        self.assertTrue(parentship_map['child'] in answer.list())
        self.assertTrue(parentship_map['parentship'] in answer.list())

        tx.close()
        local_session.close()
        inst.keyspaces().delete("shortestpath")

    def test_cluster_anwer_ConceptSet(self):
        """ Test clustering with connected components response as ConceptSet """ 
        local_session = inst.session("clusterkeyspace")
        tx = local_session.transaction(grakn.TxType.WRITE)
        parentship_map = test_Transaction._build_parentship(tx) # this closes the tx
        tx = local_session.transaction(grakn.TxType.WRITE)
        result = tx.query("compute cluster in [person, parentship], using connected-component;")
        concept_set_answer = next(result)
        self.assertIsInstance(concept_set_answer, ConceptSet)
        self.assertEqual(len(concept_set_answer.set()), 3)
        self.assertTrue(parentship_map['parent'] in concept_set_answer.set())
        self.assertTrue(parentship_map['child'] in concept_set_answer.set())
        self.assertTrue(parentship_map['parentship'] in concept_set_answer.set())
        tx.close()
        local_session.close()
        inst.keyspaces().delete("clusterkeyspace")


    def test_compute_centrality_answer_ConceptSetMeasure(self):
        """ Test compute centrality, response type ConceptSetMeasure """
        local_session = inst.session("centralitykeyspace")
        tx = local_session.transaction(grakn.TxType.WRITE)
        parentship_map = test_Transaction._build_parentship(tx) # this closes the tx
        tx = local_session.transaction(grakn.TxType.WRITE)
        result = tx.query("compute centrality in [person, parentship], using degree;")
        concept_set_measure_answer = next(result)
        self.assertIsInstance(concept_set_measure_answer, ConceptSetMeasure)
        self.assertEqual(concept_set_measure_answer.measurement(), 1)
        self.assertTrue(parentship_map['parent'] in concept_set_measure_answer.set())
        self.assertTrue(parentship_map['child'] in concept_set_measure_answer.set())
        tx.close()
        local_session.close()
        inst.keyspaces().delete("centralitykeyspace")


    def test_compute_aggregate_group_answer_AnswerGroup(self):
        """ Test compute aggreate count, response type AnwerGroup """
        local_session = inst.session("aggregategroup")
        tx = local_session.transaction(grakn.TxType.WRITE)
        parentship_map = test_Transaction._build_parentship(tx) # this closes the tx
        tx = local_session.transaction(grakn.TxType.WRITE)
        result = tx.query("match $x isa person; $y isa person; (parent: $x, child: $y) isa parentship; get; group $x;")
        answer_group = next(result)
        self.assertIsInstance(answer_group, AnswerGroup)
        self.assertEqual(answer_group.owner().id, parentship_map['parent'])
        self.assertEqual(answer_group.answers()[0].get('x').id, parentship_map['parent'])
        self.assertEqual(answer_group.answers()[0].map()['y'].id, parentship_map['child'])
        tx.close()
        local_session.close()
        inst.keyspaces().delete("aggregategroup")



    # --- test different Answers and their APIs ---



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
        when = "{ (parent: $p, child: $c) isa parentship; $p has gender 'female'; $c has gender 'male'; };"
        then = "{ (mother: $p, son: $c) isa parentship; };"

        rule = self.tx.put_rule(label, when, then)
        self.assertTrue(rule.is_rule())
        self.assertEqual(rule.label(), label)
        
if __name__ == "__main__":
    unittest.main()
