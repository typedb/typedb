const env = require('../../../support/GraknTestEnvironment');

let graknClient;
let session;

beforeAll(() => {
    graknClient = env.graknClient;
    session = graknClient.session("testcommit");
});

afterAll(async () => {
    graknClient.keyspace.delete("testcommit");
    env.tearDown();
});

describe('Integration test', () => {

    test('Open tx with invalid parameter throws error', async () => {
        await expect(session.transaction('invalidTxType')).rejects.toThrowError();
    });

    test("Tx open in READ mode should throw when trying to define", async () => {
        const tx = await session.transaction(env.txType().READ);
        await expect(tx.query("define person sub entity;")).rejects.toThrowError();
        tx.close();
    });

    test("If tx does not commit, different Tx won't see changes", async () => {
        const tx = await session.transaction(env.txType().WRITE);
        await tx.query("define superman sub entity;");
        tx.close()
        const newTx = await session.transaction(env.txType().WRITE);
        await expect(newTx.query("match $x sub superman; get;")).rejects.toThrowError(); // superman label does not exist in the graph
        newTx.close();
    });

    test("When tx commit, different tx will see changes", async () => {
        const tx = await session.transaction(env.txType().WRITE);
        await tx.query("define superman sub entity;");
        await tx.commit();
        const newTx = await session.transaction(env.txType().WRITE);
        const superman = await newTx.getSchemaConcept('superman');
        expect(superman.isSchemaConcept()).toBeTruthy();
        newTx.close();
    });

    // test("explanation", async () => {
    //     const localSession = graknClient.session("gene");
    //     const tx = await localSession.transaction(env.txType().WRITE);
    //     const iterator = await tx.query("match $x isa cousins; offset 0; limit 1; get;", { infer: true });
    //     const answer = await iterator.next();
    //     expect(answer.get().size).toBe(1);
    //     expect(answer.explanation().answers()).toHaveLength(3);
    //     tx.close()
    //     localSession.close();
    // });

    // test("no results with infer false", async () => {
    //     const localSession = graknClient.session("gene");
    //     const tx = await localSession.transaction(env.txType().WRITE);
    //     const iterator = await tx.query("match $x isa cousins; offset 0; limit 1; get;", { infer: false });
    //     const answer = await iterator.next();
    //     expect(answer).toBeNull();
    //     tx.close();
    //     localSession.close();
    // });

});




