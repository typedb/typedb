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

describe("Transaction methods", () => {

  test("getConcept", async () => {
    await tx.query("define person sub entity;");
    const iterator = await tx.query("insert $x isa person;");
    const person = (await iterator.next()).map().get('x');
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
    await tx.query("define person sub entity;");
    const result = await tx.query("match $x sub entity; get;");
    const concepts = (await result.collectConcepts());
    expect(concepts.length).toBe(2);
    const set = new Set(concepts.map(concept => concept.id));
    expect(set.size).toBe(2);
  });

  test("execute query with no results", async () => {
    await tx.query("define person sub entity;");
    const result = await tx.query("match $x isa person; get;")
    const emptyArray = await result.collect();
    expect(emptyArray).toHaveLength(0);
  });

  test("execute compute count on empty graph - Answer of Value", async () => {
    const result = await tx.query("compute count;");
    const answer = await(result.next());
    expect(answer.number()).toBe(0);
  });

  test.only("execute aggregate count on empty graph - Answer of Value", async () => {
    const result = await tx.query("match $x; aggregate count;");
    const answer = await(result.next());
    expect(answer.number()).toBe(6);
  });

  async function buildParentship(localTx){
    const relationshipType = await localTx.putRelationshipType('parentship');
    const relationship = await relationshipType.create();
    const parentRole = await localTx.putRole('parent');
    const childRole = await localTx.putRole('child');
    await relationshipType.relates(childRole);
    await relationshipType.relates(parentRole);
    const personType = await localTx.putEntityType('person');
    await personType.plays(parentRole);
    await personType.plays(childRole);
    const parent = await personType.create();
    const child = await personType.create();
    await relationship.assign(childRole, child);
    await relationship.assign(parentRole, parent);
    await localTx.commit();
    return {child: child.id, parent: parent.id, rel: relationship.id};
  }

  test("shortest path - Answer of conceptList", async ()=>{
    const localSession = env.sessionForKeyspace('shortestpathks');
    let localTx = await localSession.transaction(env.txType().WRITE);
    const parentshipMap = await buildParentship(localTx);
    localTx = await localSession.transaction(env.txType.WRITE);
    const result = await localTx.query(`compute path from ${parentshipMap.parent}, to ${parentshipMap.child};`);
    const answer = await(result.next());
    expect(answer.list()).toHaveLength(3);
    expect(answer.list().includes(parentshipMap.child)).toBeTruthy();
    expect(answer.list().includes(parentshipMap.parent)).toBeTruthy();
    expect(answer.list().includes(parentshipMap.rel)).toBeTruthy();
    localTx.close();
    localSession.close();
    env.graknClient.keyspace.delete('shortestpathks');
  });

  test("cluster connected components - Answer of conceptSet", async ()=>{
    const localSession = env.sessionForKeyspace('clusterkeyspace');
    let localTx = await localSession.transaction(env.txType().WRITE);
    const parentshipMap = await buildParentship(localTx);
    localTx = await localSession.transaction(env.txType.WRITE);
    const result = await localTx.query("compute cluster in [person, parentship], using connected-component;");
    const answer = await(result.next());
    expect(answer.set().size).toBe(3);
    expect(answer.set().has(parentshipMap.child)).toBeTruthy();
    expect(answer.set().has(parentshipMap.parent)).toBeTruthy();
    expect(answer.set().has(parentshipMap.rel)).toBeTruthy();
    localTx.close();
    localSession.close();
    env.graknClient.keyspace.delete('clusterkeyspace');
  });

  test("compute centrality - Answer of conceptSetMeasure", async ()=>{
    const localSession = env.sessionForKeyspace('computecentralityks');
    let localTx = await localSession.transaction(env.txType().WRITE);
    const parentshipMap = await buildParentship(localTx);
    localTx = await localSession.transaction(env.txType.WRITE);
    const result = await localTx.query("compute centrality in [person, parentship], using degree;");
    const answer = await(result.next());
    expect(answer.measurement()).toBe(1);
    expect(answer.set().has(parentshipMap.child)).toBeTruthy();
    expect(answer.set().has(parentshipMap.parent)).toBeTruthy();
    localTx.close();
    localSession.close();
    env.graknClient.keyspace.delete('computecentralityks');
  });

  test("compute aggregate group - Answer of answerGroup", async ()=>{
    const localSession = env.sessionForKeyspace('groupks');
    let localTx = await localSession.transaction(env.txType().WRITE);
    const parentshipMap = await buildParentship(localTx);
    localTx = await localSession.transaction(env.txType.WRITE);
    const result = await localTx.query("match $x isa person; $y isa person; (parent: $x, child: $y) isa parentship; aggregate group $x;");
    const answer = await(result.next());
    expect(answer.owner().id).toBe(parentshipMap.parent);
    expect(answer.answers()[0].map().size).toBe(2);
    expect(answer.answers()[0].map().get('x').id).toBe(parentshipMap.parent);
    expect(answer.answers()[0].map().get('y').id).toBe(parentshipMap.child);

    localTx.close();
    localSession.close();
    env.graknClient.keyspace.delete('groupks');
  });


  test("getSchemaConcept", async () => {
    await tx.query("define person sub entity;");

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
    expect(await rule.label()).toBe(label);
    expect(rule.isRule()).toBeTruthy();
  });

  test("getAttributesByValue", async () => {
    const firstNameAttributeType = await tx.putAttributeType("firstname", env.dataType().STRING);
    const middleNameAttributeType = await tx.putAttributeType("middlename", env.dataType().STRING);
    const a1 = await firstNameAttributeType.create('James');
    const a2 = await middleNameAttributeType.create('James');
    const attributes = await (await tx.getAttributesByValue('James', env.dataType().STRING)).collect();
    expect(attributes.length).toBe(2);
    expect(attributes.filter(a => a.id === a1.id).length).toBe(1);
    expect(attributes.filter(a => a.id === a2.id).length).toBe(1);
    attributes.forEach(async attr => {
      expect(attr.isAttribute()).toBeTruthy();
      expect(await attr.value()).toBe('James');
    });
    const bondAttributes = await (await tx.getAttributesByValue('Bond', env.dataType().STRING)).collect();
    expect(bondAttributes).toHaveLength(0);
  });

});
