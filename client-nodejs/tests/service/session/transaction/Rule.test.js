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

describe("Rule methods", () => {

    test("get When/Then", async () => {
        const label = "genderisedParentship";
        const when = "{ (parent: $p, child: $c) isa parentship; $c has gender \"male\"; $p has gender \"female\"; };";
        const then = "{ (mother: $p, son: $c) isa parentship; };";
        const rule = await tx.putRule(label, when, then);
        expect(await rule.getWhen()).toBe(when);
        expect(await rule.getThen()).toBe(then);
    });

    test("get When/Then when null", async () => {
        const rule = await tx.getSchemaConcept('rule');
        expect(await rule.getWhen()).toBeNull();
        expect(await rule.getThen()).toBeNull();
    });
});
