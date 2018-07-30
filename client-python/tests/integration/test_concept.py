import unittest
import grakn


# run-once per testing
inst = grakn.Grakn("localhost:48555")
session = inst.session("testkeyspace")

# shared grakn instances and session for API testing 
inst = grakn.Grakn("localhost:48555")
session = inst.session("testkeyspace")

class test_Base(unittest.TestCase):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        """ Make sure we have some sort of schema and data in DB, only done once """
        super(test_Base, cls).setUpClass()

        # temp tx to set up DB, don"t save it
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



class test_Concept(test_Base):
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

        # TODO refine exception
        with self.assertRaises(Exception):
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

class test_SchemaConcept(test_Base):
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

        # try setting an invalid label
        with self.subTest(i=2):
            bike_type = self.tx.get_schema_concept("bike")
            #TODO specialize exception
            with self.assertRaises(Exception):
                bike_type.label("")
            with self.assertRaises(Exception):
                bike_type.label(100)


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










    
if __name__ == "__main__":
    unittest.main()

    
