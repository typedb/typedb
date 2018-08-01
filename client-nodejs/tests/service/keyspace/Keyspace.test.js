const env = require('../../support/GraknTestEnvironment');
let graknClient;

beforeAll(() => {
    graknClient = env.graknClient;
});

afterAll(async () => {
    await env.tearDown();
});

describe("Keyspace methods", () => {

    test("retrieve and delete", async () => {
        const session = graknClient.session("retrievetest");
        const tx = await session.transaction(env.txType().WRITE);
        tx.close();
        const keyspaces = await graknClient.keyspace.retrieve();
        expect(keyspaces.length).toBeGreaterThan(0);
        expect(keyspaces.includes('retrievetest')).toBeTruthy;
        await graknClient.keyspace.delete('retrievetest');
        const keyspacesAfterDeletion = await graknClient.keyspace.retrieve();
        expect(keyspacesAfterDeletion.includes('retrievetest')).toBeFalsy();
        session.close();
    });

});