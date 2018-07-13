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

describe("Attribute type methods", () => {

    test("putAttribute", async () => {
        const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
        const attribute = await attributeType.putAttribute('Marco');
        expect(attribute.isAttribute()).toBeTruthy();
        expect(await attribute.getValue()).toBe('Marco');

        const boolAttributeType = await tx.putAttributeType("employed", env.dataType().BOOLEAN);
        const boolAttribute = await boolAttributeType.putAttribute(false);
        expect(await boolAttribute.getValue()).toBe(false);

        const doubleAttributeType = await tx.putAttributeType("length", env.dataType().DOUBLE);
        const doubleAttribute = await doubleAttributeType.putAttribute(11.58);
        expect(await doubleAttribute.getValue()).toBe(11.58);
    });

    test('getDataType', async () => {
        const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
        expect(await attributeType.getDataType()).toBe('String');

        const boolAttributeType = await tx.putAttributeType("employed", env.dataType().BOOLEAN);
        expect(await boolAttributeType.getDataType()).toBe('Boolean');

        const doubleAttributeType = await tx.putAttributeType("length", env.dataType().DOUBLE);
        expect(await doubleAttributeType.getDataType()).toBe('Double');
    });

    test('getAttribute', async () => {
        const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
        await attributeType.putAttribute('Marco');
        const attribute = await attributeType.getAttribute('Marco');
        expect(attribute.isAttribute()).toBeTruthy();
        const nullAttribute = await attributeType.getAttribute('Giangiovannino');
        expect(nullAttribute).toBeNull();
    });

    test('set/get regex', async () => {
        const attributeType = await tx.putAttributeType("id", env.dataType().STRING);
        const emptyRegex = await attributeType.getRegex();
        expect(emptyRegex).toHaveLength(0);

        await attributeType.setRegex("(good|bad)-dog");
        const regex = await attributeType.getRegex();

        expect(regex).toBe("(good|bad)-dog");
    });
});