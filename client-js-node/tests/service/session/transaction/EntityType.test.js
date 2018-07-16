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

describe("Entity type methods", () => {

    test("create", async () => {
        const personType = await tx.putEntityType("person");
        const person = await personType.create();
        expect(person.isEntity()).toBeTruthy();
    });
});