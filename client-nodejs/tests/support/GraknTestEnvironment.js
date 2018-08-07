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

const DEFAULT_URI = "localhost:48555";
const INTEGRATION_TESTS_TIMEOUT = 40000;
const TEST_KEYSPACE = 'testkeyspace';

// Test Grakn with distribution code if TEST_ENV is dist
let Grakn;
let graknClient;
if(process.env.TEST_ENV === 'dist'){
    Grakn = require("../../dist/Grakn");
    graknClient = new Grakn(DEFAULT_URI);
}else {
    Grakn = require("../../src/Grakn");
    graknClient = new Grakn(DEFAULT_URI);
}
//Every test file instantiate a new GraknEnvironment - so session will be new for every test file
const session = graknClient.session(TEST_KEYSPACE);

jest.setTimeout(INTEGRATION_TESTS_TIMEOUT);

module.exports = {
    session: () => session,
    sessionForKeyspace: (keyspace) => graknClient.session(keyspace),
    tearDown: async () => {
        await session.close();
        // await graknClient.keyspaces().delete(TEST_KEYSPACE);
    },
    dataType: () => Grakn.dataType,
    txType: () => Grakn.txType,
    graknClient
}