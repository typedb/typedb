const env = require('./support/GraknTestEnvironment');
let session;
let tx;

beforeAll(() => {
    session = env.session();
});

afterAll(async () => {
    await env.tearDown();
});

beforeEach(async () => {
    tx = await session.transaction(env.txType().WRITE);
})

afterEach(() => {
    tx.close();
});

describe("Role methods", () => {

    test("relationshipTypes", async () => {
        await tx.execute('define parentship sub relationship, relates parent, relates child;');
        const result = await tx.execute('match $x label parent; get;');
        const concepts = (await result.collectAll()).map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
        const role = concepts[0];
        expect(role.baseType).toBe('ROLE');
        const rels = await (await role.relationshipTypes()).collectAll();
        expect(rels[0].baseType).toBe('RELATIONSHIP_TYPE');
        expect(await rels[0].getLabel()).toBe('parentship');
    });

    test("playedByTypes", async () => {
        await tx.execute('define parentship sub relationship, relates parent, relates child;');
        await tx.execute('define person sub entity plays parent;')
        const result = await tx.execute('match $x label parent; get;');
        const concepts = (await result.collectAll()).map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
        const role = concepts[0];
        expect(role.baseType).toBe('ROLE');
        const types = await (await role.playedByTypes()).collectAll();
        expect(types[0].baseType).toBe('ENTITY_TYPE');
        expect(await types[0].getLabel()).toBe('person');
    });
});
