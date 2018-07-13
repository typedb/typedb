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

describe("Attribute methods", () => {

    test("getValue", async () => {
        const doubleAttributeType = await tx.putAttributeType("length", env.dataType().DOUBLE);
        const doubleAttribute = await doubleAttributeType.putAttribute(11.58);
        expect(await doubleAttribute.getValue()).toBe(11.58);
    });

    // TODO rewrite test when fixed on the server 
    // test.only("getValue Date", async () => {
    //     const dateType = await tx.putAttributeType("birth-date", env.dataType().DATE);
    //     const personType = await tx.putEntityType('person');
    //     await personType.attribute(dateType);

    //     const insertionResult = await tx.execute("insert $x isa person, has birth-date 2018-08-06;");
    //     const concepts = insertionResult.map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
    //     const person = concepts[0];
    //     const attrs = await person.attributes();
    //     const date = attrs[0];
    //     // const setter = (new Date('2018-08-06')).getTime() / 1000;
    //     const value = await date.getValue();
    //     const dateNew = await dateType.putAttribute(value);
    // });

    test("ownerInstances", async () => {
        const personType = await tx.putEntityType('person');
        const animalType = await tx.putEntityType('animal');
        const nameType = await tx.putAttributeType("name", env.dataType().STRING);
        await personType.attribute(nameType);
        await animalType.attribute(nameType);
        const person = await personType.addEntity();
        const dog = await animalType.addEntity();
        const name = await nameType.putAttribute('Giacobbe');
        await person.attribute(name);
        await dog.attribute(name);

        const owners = await (await name.ownerInstances()).collectAll();
        expect(owners.length).toBe(2);
        const ids = [person.id, dog.id];
        const ownersIds = owners.map(x => x.id);
        ids.sort();
        ownersIds.sort();
        expect(ids[0]).toBe(ownersIds[0]);
        expect(ids[1]).toBe(ownersIds[1]);
    });

});