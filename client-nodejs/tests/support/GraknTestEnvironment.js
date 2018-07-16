const Grakn = require("../../src/Grakn");

const DEFAULT_URI = "localhost:48555";
const INTEGRATION_TESTS_TIMEOUT = 20000;
const TEST_KEYSPACE = 'testkeyspace';

function newKeyspace() {
    const randomName = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    return 'a' + randomName;
}
const graknClient = new Grakn(DEFAULT_URI);
const session = graknClient.session(TEST_KEYSPACE);

jest.setTimeout(INTEGRATION_TESTS_TIMEOUT);

module.exports = {
    newKeyspace,
    session: () => session,
    tearDown: async () => {
        await graknClient.keyspace.delete(TEST_KEYSPACE);
        graknClient.close();
    },
    dataType: () => Grakn.dataType,
    txType: () => Grakn.txType,
    graknClient
}