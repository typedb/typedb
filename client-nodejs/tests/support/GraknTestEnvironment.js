const DEFAULT_URI = "localhost:48555";
const INTEGRATION_TESTS_TIMEOUT = 40000;
const TEST_KEYSPACE = 'testkeyspace';

// Test Grakn with distribution code if TEST_ENV is dist
let Grakn;
let graknClient;
if(process.env.TEST_ENV === 'dist'){
    Grakn = require("../../dist/Grakn");
    graknClient = new DistGrakn(DEFAULT_URI);
}else {
    Grakn = require("../../src/Grakn");
    graknClient = new Grakn(DEFAULT_URI);
}
//Every test file instantiate a new GraknEnvironment - so session will be new for every test file
const session = graknClient.session(TEST_KEYSPACE);

jest.setTimeout(INTEGRATION_TESTS_TIMEOUT);

module.exports = {
    session: () => session,
    sessionForKeyspace: (keyspace) => graknClient.session(keyspace),
    tearDown: async () => {
        await session.close();
        // await graknClient.keyspace.delete(TEST_KEYSPACE);
    },
    dataType: () => Grakn.dataType,
    txType: () => Grakn.txType,
    graknClient
}