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

describe("Attribute methods", () => {

    test("value", async () => {
        const doubleAttributeType = await tx.putAttributeType("length", env.dataType().DOUBLE);
        const doubleAttribute = await doubleAttributeType.create(11.58);
        expect(await doubleAttribute.value()).toBe(11.58);
    });

    // TODO rewrite test when fixed on the server 
    // test.only("getValue Date", async () => {
    //     const dateType = await tx.putAttributeType("birth-date", env.dataType().DATE);
    //     const personType = await tx.putEntityType('person');
    //     await personType.attribute(dateType);

    //     const insertionResult = await tx.query("insert $x isa person, has birth-date 2018-08-06;");
    //     const concepts = insertionResult.map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
    //     const person = concepts[0];
    //     const attrs = await person.attributes();
    //     const date = attrs[0];
    //     // const setter = (new Date('2018-08-06')).getTime() / 1000;
    //     const value = await date.getValue();
    //     const dateNew = await dateType.putAttribute(value);
    // });

    test("owners", async () => {
        const personType = await tx.putEntityType('person');
        const animalType = await tx.putEntityType('animal');
        const nameType = await tx.putAttributeType("name", env.dataType().STRING);
        await personType.has(nameType);
        await animalType.has(nameType);
        const person = await personType.create();
        const dog = await animalType.create();
        const name = await nameType.create('Giacobbe');
        await person.has(name);
        await dog.has(name);

        const owners = await (await name.owners()).collectAll();
        expect(owners.length).toBe(2);
        const ids = [person.id, dog.id];
        const ownersIds = owners.map(x => x.id);
        ids.sort();
        ownersIds.sort();
        expect(ids[0]).toBe(ownersIds[0]);
        expect(ids[1]).toBe(ownersIds[1]);
    });

});