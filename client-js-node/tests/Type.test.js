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

describe("Type methods", () => {

    test("setAbstract && isAbstract", async () => {
        const dogType = await tx.putEntityType("dog");
        let isAbstract = await dogType.isAbstract();
        expect(isAbstract).toBeFalsy();
        await dogType.setAbstract(true);
        isAbstract = await dogType.isAbstract();
        expect(isAbstract).toBeTruthy();
        await dogType.setAbstract(false);
        isAbstract = await dogType.isAbstract();
        expect(isAbstract).toBeFalsy();
    });

    test("get/set/delete plays", async () => {
        const role = await tx.putRole('father');
        const type = await tx.putEntityType('person');
        const plays = await type.plays();
        expect(plays.length).toBe(0);
        await type.plays(role);
        const playsWithRole = await type.plays();
        expect(playsWithRole.length).toBe(1);
        expect(playsWithRole[0].baseType).toBe('ROLE');
        await type.deletePlays(role);
        const playsRemoved = await type.plays();
        expect(playsRemoved.length).toBe(0);
    });

    test("get/set/delete attributes", async () => {
        const type = await tx.putEntityType('person');
        const nameType = await tx.putAttributeType('name', env.dataType().STRING);
        const attrs = await type.attributes();
        expect(attrs.length).toBe(0);
        await type.attribute(nameType);
        const attrsWithName = await type.attributes();
        expect(attrsWithName.length).toBe(1);
        expect(attrsWithName[0].baseType).toBe('ATTRIBUTE_TYPE');
        await type.deleteAttribute(nameType);
        const attrsRemoved = await type.attributes();
        expect(attrsRemoved.length).toBe(0);
    });

    test("instances", async () => {
        const personType = await tx.putEntityType("person");
        const instances = await personType.instances();
        expect(instances.length).toBe(0);
        await personType.addEntity();
        const instancesWithPerson = await personType.instances();
        expect(instancesWithPerson.length).toBe(1);
    });

    test("Get/set/delete key", async () => {
        const type = await tx.putEntityType('person');
        const nameType = await tx.putAttributeType('name', env.dataType().STRING);
        const keys = await type.keys();
        expect(keys.length).toBe(0);
        await type.key(nameType);
        const keysWithName = await type.keys();
        expect(keysWithName.length).toBe(1);
        expect(keysWithName[0].baseType).toBe('ATTRIBUTE_TYPE');
        await type.deleteKey(nameType);
        const keysRemoved = await type.keys();
        expect(keysRemoved.length).toBe(0);
    });
});

