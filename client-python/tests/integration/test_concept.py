import unittest
import grakn


# run-once per testing
inst = grakn.Grakn('localhost:48555')
session = inst.session("testkeyspace")

class test_Base(unittest.TestCase):
    def setUp(self):
        self.tx = session.transaction(grakn.TxType.WRITE)

    def tearDown(self):
        self.tx.close()

class test_Concept(test_Base):
    """ Test methods available on all Concepts """

    def test_delete_schema_types(self):
        car_type = self.tx.put_entity_type('car')
        
        schema_concept = self.tx.get_schema_concept('car')
        self.assertTrue(schema_concept.is_schema_concept())
        schema_concept.delete()
        none_schema_car = self.tx.get_schema_concept('car')
        self.assertIsNone(none_schema_car, msg="Deletion of car schema type failed")

    def test_delete_instance(self):
        car_type = self.tx.put_entity_type('car')
        car = car_type.create()

        car.delete()
        none_car = self.tx.get_concept(car.id)
        self.assertIsNone(none_car, msg="Deletion of car instance failed")

    def test_re_delete_instance(self):
        car_type = self.tx.put_entity_type('car')
        car = car_type.create()
        
        car.delete()
        none_car = self.tx.get_concept(car.id)
        self.assertIsNone(none_car)

        # TODO refine exception
        with self.assertRaises(Exception):
            car.delete()

    
    def test_is_each_schema_type(self):
        car_type = self.tx.put_entity_type('car')
        car = car_type.create()
        self.assertTrue(car.is_entity())
        self.assertFalse(car.is_attribute())
        self.assertFalse(car.is_relationship())

        rel_type = self.tx.put_relationship_type('owner')
        owner = rel_type.create()
        self.assertFalse(owner.is_entity())
        self.assertFalse(owner.is_attribute())
        self.assertTrue(owner.is_relationship())

        attr_type = self.tx.put_attribute_type('age', grakn.DataType.LONG)
        age = attr_type.create(50)
        self.assertFalse(age.is_entity())
        self.assertTrue(age.is_attribute())
        self.assertFalse(age.is_relationship())



    
if __name__ == "__main__":
    unittest.main()

    
