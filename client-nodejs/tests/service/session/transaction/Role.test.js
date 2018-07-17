const env = require('../../../support/GraknTestEnvironment');
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

    test("relationships", async () => {
        await tx.query('define parentship sub relationship, relates parent, relates child;');
        const result = await tx.query('match $x label parent; get;');
        const concepts = (await result.collect()).map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
        const role = concepts[0];
        expect(role.baseType).toBe('ROLE');
        const rels = await (await role.relationships()).collect();
        expect(rels[0].baseType).toBe('RELATIONSHIP_TYPE');
        expect(await rels[0].label()).toBe('parentship');
    });

    test("players", async () => {
        await tx.query('define parentship sub relationship, relates parent, relates child;');
        await tx.query('define person sub entity plays parent;')
        const result = await tx.query('match $x label parent; get;');
        const concepts = (await result.collect()).map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
        const role = concepts[0];
        expect(role.baseType).toBe('ROLE');
        const types = await (await role.players()).collect();
        expect(types[0].baseType).toBe('ENTITY_TYPE');
        expect(await types[0].label()).toBe('person');
    });
});
