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

describe("Relationship type methods", () => {

    test("create", async () => {
        const relationshipType = await tx.putRelationshipType("parenthood");
        const relationship = await relationshipType.create();
        expect(relationship.isRelationship()).toBeTruthy();
    });

    test('Get/set/delete relates', async () => {
        const relationshipType = await tx.putRelationshipType("parenthood");
        const parentRole = await tx.putRole('parent');
        const childRole = await tx.putRole('child');
        const relates = await (await relationshipType.roles()).collect();
        expect(relates.length).toBe(0);
        await relationshipType.relates(parentRole);
        await relationshipType.relates(childRole);
        const populateRelates = await (await relationshipType.roles()).collect();
        expect(populateRelates.length).toBe(2);
        await relationshipType.unrelate(parentRole);
        const oneRole = await (await relationshipType.roles()).collect();
        expect(oneRole.length).toBe(1);
        expect(oneRole[0].baseType).toBe('ROLE');
    });
});