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

describe("Attribute type methods", () => {

    test("create", async () => {
        const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
        const attribute = await attributeType.create('Marco');
        expect(attribute.isAttribute()).toBeTruthy();
        expect(await attribute.value()).toBe('Marco');

        const boolAttributeType = await tx.putAttributeType("employed", env.dataType().BOOLEAN);
        const boolAttribute = await boolAttributeType.create(false);
        expect(await boolAttribute.value()).toBe(false);

        const doubleAttributeType = await tx.putAttributeType("length", env.dataType().DOUBLE);
        const doubleAttribute = await doubleAttributeType.create(11.58);
        expect(await doubleAttribute.value()).toBe(11.58);
    });

    test('dataType', async () => {
        const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
        expect(await attributeType.dataType()).toBe('String');

        const boolAttributeType = await tx.putAttributeType("employed", env.dataType().BOOLEAN);
        expect(await boolAttributeType.dataType()).toBe('Boolean');

        const doubleAttributeType = await tx.putAttributeType("length", env.dataType().DOUBLE);
        expect(await doubleAttributeType.dataType()).toBe('Double');
    });

    test('attribute', async () => {
        const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
        await attributeType.create('Marco');
        const attribute = await attributeType.attribute('Marco');
        expect(attribute.isAttribute()).toBeTruthy();
        const nullAttribute = await attributeType.attribute('Giangiovannino');
        expect(nullAttribute).toBeNull();
    });

    test('set/get regex', async () => {
        const attributeType = await tx.putAttributeType("id", env.dataType().STRING);
        const emptyRegex = await attributeType.regex();
        expect(emptyRegex).toHaveLength(0);

        await attributeType.regex("(good|bad)-dog");
        const regex = await attributeType.regex();

        expect(regex).toBe("(good|bad)-dog");
    });
});