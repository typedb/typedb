import unittest
import grakn

client = grakn.Grakn("localhost:48555")

class test_Keyspace(unittest.TestCase):

    def test_retrieve_delete(self):
        """ Test retrieving and deleting a specific keyspace """

        session = client.session(keyspace="keyspacetest")
        tx = session.transaction(grakn.TxType.WRITE)
        tx.close()

        keyspaces = client.keyspace.retrieve()
        self.assertGreater(len(keyspaces), 0)
        self.assertTrue('keyspacetest' in keyspaces)

        client.keyspace.delete('keyspacetest')
        post_delete_keyspaces = client.keyspace.retrieve()
        self.assertFalse('keyspacetest' in post_delete_keyspaces)

        session.close()

