const Grakn = require("../../src/Grakn");

const DEFAULT_URI = "localhost:48555";
const INTEGRATION_TESTS_TIMEOUT = 20000;
const TEST_KEYSPACE = 'testkeyspace';

const graknClient = new Grakn(DEFAULT_URI);
//Every test file instantiate a new GraknEnvironment - so session will be new for every test file
const session = graknClient.session(TEST_KEYSPACE);

jest.setTimeout(INTEGRATION_TESTS_TIMEOUT);

module.exports = {
    session: () => session,
    tearDown: async () => {
        await session.close();
        // await graknClient.keyspace.delete(TEST_KEYSPACE);
    },
    dataType: () => Grakn.dataType,
    txType: () => Grakn.txType,
    graknClient
}