/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.test.behaviour.resolution.framework.test;

import com.vaticle.typedb.core.TypeDB.Session;

import static com.vaticle.typedb.core.test.common.TypeQLTestUtil.loadFromFileAndCommit;


public class LoadTest {

    static void loadTestStub(Session session, String testStub) {
        String stubsPath = "test/behaviour/resolution/framework/test/stubs/";
        String path = stubsPath + testStub + "/";
        // Load a schema incl. rules
        loadFromFileAndCommit(path, "schema.gql", session);
        // Load data
        loadFromFileAndCommit(path, "data.gql", session);
    }
}
