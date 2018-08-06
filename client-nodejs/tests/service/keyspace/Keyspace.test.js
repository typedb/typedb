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

const env = require('../../support/GraknTestEnvironment');
let graknClient;

beforeAll(() => {
    graknClient = env.graknClient;
});

afterAll(async () => {
    await env.tearDown();
});

describe("Keyspace methods", () => {

    test("retrieve and delete", async () => {
        const session = graknClient.session("retrievetest");
        const tx = await session.transaction(env.txType().WRITE);
        tx.close();
        const keyspaces = await graknClient.keyspace.retrieve();
        expect(keyspaces.length).toBeGreaterThan(0);
        expect(keyspaces.includes('retrievetest')).toBeTruthy;
        await graknClient.keyspace.delete('retrievetest');
        const keyspacesAfterDeletion = await graknClient.keyspace.retrieve();
        expect(keyspacesAfterDeletion.includes('retrievetest')).toBeFalsy();
        session.close();
    });

});