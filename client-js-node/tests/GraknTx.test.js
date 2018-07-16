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

describe("GraknTx methods", () => {

  test("getConcept", async () => {
    await tx.execute("define person sub entity;");
    const iterator = await tx.execute("insert $x isa person;");
    // const concepts = iterator.map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
    // expect(concepts.length).toBe(1);
    const person = (await iterator.next()).get('x');
    const personId = person.id;

    const samePerson = await tx.getConcept(personId);
    expect(samePerson.isThing()).toBeTruthy();
    expect(samePerson.id).toBe(personId);

    // retrieve non existing id should return null
    const nonPerson = await tx.getConcept("not-existing-id");
    expect(nonPerson).toBe(null);
  });

  // Bug regression test
  test("Ensure no duplicates in metatypes", async () => {
    await tx.execute("define person sub entity;");
    const result = await tx.execute("match $x sub entity; get;");
    const concepts = (await result.collectAll()).map(map => Array.from(map.values())).reduce((a, c) => a.concat(c), []);
    expect(concepts.length).toBe(2);
    const set = new Set(concepts.map(concept => concept.id));
    expect(set.size).toBe(2);
  });

  test("execute query with no results", async () => {
    await tx.execute("define person sub entity;");
    const result = await tx.execute("match $x isa person; get;")
    const emptyArray = await result.collectAll();
    expect(emptyArray).toHaveLength(0);
  });

  test("getSchemaConcept", async () => {
    await tx.execute("define person sub entity;");

    const personType = await tx.getSchemaConcept("person");
    expect(personType.isSchemaConcept()).toBeTruthy();

    const nonPerson = await tx.getSchemaConcept("not-existing-label");
    expect(nonPerson).toBe(null);

  });

  test("putEntityType", async () => {
    const personType = await tx.putEntityType("person");
    expect(personType.isSchemaConcept()).toBeTruthy();
    expect(personType.isEntityType()).toBeTruthy();
  });

  test("putRelationshipType", async () => {
    const marriage = await tx.putRelationshipType("marriage");
    expect(marriage.isSchemaConcept()).toBeTruthy();
    expect(marriage.isRelationshipType()).toBeTruthy();
  });

  test("putAttributeType", async () => {
    const attributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
    expect(attributeType.isAttributeType()).toBeTruthy();
  });

  test("putRole", async () => {
    const role = await tx.putRole("father");
    expect(role.isRole()).toBeTruthy();
    expect(role.baseType).toBe("ROLE");
  });

  test("putRule", async () => {
    const label = "genderisedParentship";
    const when = "{(parent: $p, child: $c) isa parentship; $p has gender 'female'; $c has gender 'male';}"
    const then = "{(mother: $p, son: $c) isa parentship;}";
    const rule = await tx.putRule(label, when, then);
    expect(await rule.getLabel()).toBe(label);
    expect(rule.isRule()).toBeTruthy();
  });

  test("getAttributesByValue", async () => {
    const firstNameAttributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
    const middleNameAttributeType = await tx.putAttributeType("middlename", env.dataType().STRING);
    const a1 = await firstNameAttributeType.putAttribute('James');
    const a2 = await middleNameAttributeType.putAttribute('James');
    const attributes = await (await tx.getAttributesByValue('James', env.dataType().STRING)).collectAll();
    expect(attributes.length).toBe(2);
    expect(attributes.filter(a => a.id === a1.id).length).toBe(1);
    expect(attributes.filter(a => a.id === a2.id).length).toBe(1);
    attributes.forEach(async attr => {
      expect(attr.isAttribute()).toBeTruthy();
      expect(await attr.getValue()).toBe('James');
    });
  });
});
