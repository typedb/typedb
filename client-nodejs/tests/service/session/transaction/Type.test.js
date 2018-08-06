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

describe("Type methods", () => {

    test("isAbstract", async () => {
        const dogType = await tx.putEntityType("dog");
        let isAbstract = await dogType.isAbstract();
        expect(isAbstract).toBeFalsy();
        await dogType.isAbstract(true);
        isAbstract = await dogType.isAbstract();
        expect(isAbstract).toBeTruthy();
        await dogType.isAbstract(false);
        isAbstract = await dogType.isAbstract();
        expect(isAbstract).toBeFalsy();
    });

    test("get/set/delete plays", async () => {
        const role = await tx.putRole('father');
        const type = await tx.putEntityType('person');
        const plays = await (await type.playing()).collect();
        expect(plays.length).toBe(0);
        await type.plays(role);
        const playsWithRole = await (await type.playing()).collect();
        expect(playsWithRole.length).toBe(1);
        expect(playsWithRole[0].baseType).toBe('ROLE');
        await type.unplay(role);
        const playsRemoved = await (await type.playing()).collect();
        expect(playsRemoved.length).toBe(0);
    });

    test("get/set/delete attributes", async () => {
        const type = await tx.putEntityType('person');
        const nameType = await tx.putAttributeType('name', env.dataType().STRING);
        const attrs = await (await type.attributes()).collect();
        expect(attrs.length).toBe(0);
        await type.has(nameType);
        const attrsWithName = await (await type.attributes()).collect();
        expect(attrsWithName.length).toBe(1);
        expect(attrsWithName[0].baseType).toBe('ATTRIBUTE_TYPE');
        await type.unhas(nameType);
        const attrsRemoved = await (await type.attributes()).collect();
        expect(attrsRemoved.length).toBe(0);
    });

    test("instances", async () => {
        const personType = await tx.putEntityType("person");
        const instances = await (await personType.instances()).collect();
        expect(instances.length).toBe(0);
        await personType.create();
        const instancesWithPerson = await (await personType.instances()).collect();
        expect(instancesWithPerson.length).toBe(1);
    });

    test("Get/set/delete key", async () => {
        const type = await tx.putEntityType('person');
        const nameType = await tx.putAttributeType('name', env.dataType().STRING);
        const keys = await (await type.keys()).collect();
        expect(keys.length).toBe(0);
        await type.key(nameType);
        const keysWithName = await (await type.keys()).collect();
        expect(keysWithName.length).toBe(1);
        expect(keysWithName[0].baseType).toBe('ATTRIBUTE_TYPE');
        await type.unkey(nameType);
        const keysRemoved = await (await type.keys()).collect();
        expect(keysRemoved.length).toBe(0);
    });
});

