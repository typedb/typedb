/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
    const person = (await iterator.next()).get().get('x');
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
