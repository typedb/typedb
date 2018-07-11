const Grakn = require("../src/Grakn");
const env = require('./support/GraknTestEnvironment');
const DEFAULT_URI = "localhost:48555";


let session;

beforeAll(() => {
    session = Grakn.session(DEFAULT_URI, env.newKeyspace());
});

afterAll(async () => {
    await session.deleteKeyspace();
    session.close();
});

describe('Integration test', () => {

    test('Open tx with invalid parameter throws error', async () => {
        await expect(session.transaction('invalidTxType')).rejects.toThrowError();
    });

    test("Tx open in READ mode should throw when trying to define", async () => {
        const tx = await session.transaction(env.txType().READ);
        await expect(tx.execute("define person sub entity;")).rejects.toThrowError();
        tx.close();
    });

    test("If tx does not commit, different Tx won't see changes", async () => {
        const tx = await session.transaction(env.txType().WRITE);
        await tx.execute("define superman sub entity;");
        tx.close()
        const newTx = await session.transaction(env.txType().WRITE);
        await expect(newTx.execute("match $x sub superman; get;")).rejects.toThrowError(); // superman label does not exist in the graph
        newTx.close();
    });

    test("When tx commit, different tx will see changes", async () => {
        const tx = await session.transaction(env.txType().WRITE);
        await tx.execute("define superman sub entity;");
        tx.commit();
        const newTx = await session.transaction(env.txType().WRITE);
        const superman = await newTx.getSchemaConcept('superman');
        expect(superman.isSchemaConcept()).toBeTruthy();
        newTx.close();
    });

});




