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

let graknClient;
let session;

beforeAll(() => {
    graknClient = env.graknClient;
    session = graknClient.session("testcommit");
});

afterAll(async () => {
    await session.close();
    graknClient.keyspaces().delete("testcommit");
    env.tearDown();
});

describe('Integration test', () => {

    test('Open tx with invalid parameter throws error', async () => {
        await expect(session.transaction('invalidTxType')).rejects.toThrowError();
    });

    test("Tx open in READ mode should throw when trying to define", async () => {
        const tx = await session.transaction(env.txType().READ);
        await expect(tx.query("define person sub entity;")).rejects.toThrowError();
        tx.close();
    });

    test("If tx does not commit, different Tx won't see changes", async () => {
        const tx = await session.transaction(env.txType().WRITE);
        await tx.query("define catwoman sub entity;");
        tx.close()
        const newTx = await session.transaction(env.txType().WRITE);
        await expect(newTx.query("match $x sub catwoman; get;")).rejects.toThrowError(); // catwoman label does not exist in the graph
        newTx.close();
    });

    test("When tx commit, different tx will see changes", async () => {
        const tx = await session.transaction(env.txType().WRITE);
        await tx.query("define superman sub entity;");
        await tx.commit();
        const newTx = await session.transaction(env.txType().WRITE);
        const superman = await newTx.getSchemaConcept('superman');
        expect(superman.isSchemaConcept()).toBeTruthy();
        newTx.close();
    });

    test("explanation and default of infer is true", async () => {
        const localSession = graknClient.session("gene");
        const tx = await localSession.transaction(env.txType().WRITE);
        const iterator = await tx.query("match $x isa cousins; offset 0; limit 1; get;");
        const answer = await iterator.next();
        expect(answer.map().size).toBe(1);
        expect(answer.explanation().answers()).toHaveLength(3);
        expect(answer.explanation().queryPattern()).toBe("{$x isa cousins;}");
        await tx.close()
        await localSession.close();
    });

    test("explanation with join explanation", async () => {
        const localSession = graknClient.session("gene");
        const tx = await localSession.transaction(env.txType().WRITE);
        const iterator = await tx.query(`match ($x, $y) isa marriage; ($y, $z) isa marriage;
                                            $x != $z; get;`);
        const answers = await iterator.collect();
        expect(answers).toHaveLength(4);
        answers.forEach(a=>{
            expect(a.explanation).toBeDefined()
        });
        await tx.close()
        await localSession.close();
    });

    test("no results with infer false", async () => {
        const localSession = graknClient.session("gene");
        const tx = await localSession.transaction(env.txType().WRITE);
        const iterator = await tx.query("match $x isa cousins; offset 0; limit 1; get;", { infer: false });
        const answer = await iterator.next();
        expect(answer).toBeNull();
        await tx.close();
        await localSession.close();
    });

});




