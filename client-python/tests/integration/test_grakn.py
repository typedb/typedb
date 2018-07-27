import unittest
import grakn

class test_PreDbSetup(unittest.TestCase):

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
            tx.transaction(grakn.TxType.READ)

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


class test_Base(unittest.TestCase):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        """ Make sure we have some sort of schema and data in DB, only done once """
        super(test_Base, cls).setUpClass()
        inst = grakn.Grakn('localhost:48555')
        session = inst.session('testkeyspace')

        # temp tx to set up DB, don't save it
        tx = session.transaction(grakn.DataType.WRITE)
        try:
            tx.query("define person sub entity, has age; age sub attribute datatype long;")
        except Exception: # TODO specialize exception
            pass

        answers = list(tx.query("match $x isa person, has age 20;"))
        if len(answers) == 0:
            tx.query("insert $ isa person, has age 50;")
        tx.commit()

    def setUp(self):
        self.inst = grakn.Grakn('localhost:48555')
        self.session = self.inst.session('testkeyspace')
        self.tx = self.session.transaction(grakn.DataType.WRITE)



class test_Transaction(test_Base):

    # --- query tests ---
    def query_valid_result(self):
        answers = self.tx.query("match $x isa person; get;")
        self.assertIsNotNone(answers)


    def query_empty_result(self):
       pass 

    def query_invalid_syntax(self):
        pass

    def query_tx_already_closed(self):
        pass

    # --- commit tests --- 
    def commit_no_error_thrown(self):
        pass

    def commit_normal(self):
        pass

    def commit_check_tx_closed(self):
        pass

    # --- close tests ---

    def close_check_closed(self):
        # attempt to perform a query/put etc
        pass




