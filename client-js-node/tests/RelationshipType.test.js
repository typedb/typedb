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

describe("Relationship type methods", () => {

    test("addRelationship", async () => {
        const relationshipType = await tx.putRelationshipType("parenthood");
        const relationship = await relationshipType.addRelationship();
        expect(relationship.isRelationship()).toBeTruthy();
    });

    test('Get/set/delete relates', async () => {
        const relationshipType = await tx.putRelationshipType("parenthood");
        const parentRole = await tx.putRole('parent');
        const childRole = await tx.putRole('child');
        const relates = await relationshipType.relates();
        expect(relates.length).toBe(0);
        await relationshipType.relates(parentRole);
        await relationshipType.relates(childRole);
        const populateRelates = await relationshipType.relates();
        expect(populateRelates.length).toBe(2);
        await relationshipType.deleteRelates(parentRole);
        const oneRole = await relationshipType.relates();
        expect(oneRole.length).toBe(1);
        expect(oneRole[0].baseType).toBe('ROLE');
    });
});