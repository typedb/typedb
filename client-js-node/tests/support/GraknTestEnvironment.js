const Grakn = require("../../src/Grakn");

const DEFAULT_URI = "localhost:48555";
const INTEGRATION_TESTS_TIMEOUT = 2000000;
const TEST_KEYSPACE = 'testkeyspace';

function newKeyspace() {
    const randomName = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    return 'a' + randomName;
}

const session = Grakn.session(DEFAULT_URI, TEST_KEYSPACE);

jest.setTimeout(INTEGRATION_TESTS_TIMEOUT);

module.exports = {
    beforeAll: function () {

    },
    afterAll: function () {

    },
    newKeyspace,
    session: () => session,
    tearDown: async () => {
        // await session.deleteKeyspace();
        session.close();
    },
    dataType: () => Grakn.dataType,
    txType: () => Grakn.txType
}