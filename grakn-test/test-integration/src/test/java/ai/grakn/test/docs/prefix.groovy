/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.test.docs

import ai.grakn.*
import ai.grakn.concept.*
import ai.grakn.engine.tasks.mock.ShortExecutionMockTask

import java.time.*
import ai.grakn.graql.*
import ai.grakn.graql.admin.*
import ai.grakn.migration.csv.*
import ai.grakn.client.*
import ai.grakn.engine.*
import mjson.Json

import static ai.grakn.graql.Graql.*

// This is some dumb stuff so IntelliJ doesn't get rid of the imports
//noinspection GroovyConstantIfStatement
if (false) {
    Answer answer = null
    Concept concept = null
    Var var = var()
    GraknSession session = null
    LocalDateTime time = null
    CSVMigrator migrator = null
    BatchExecutorClient client = null
    Json json = null
    TaskId id = null
    ShortExecutionMockTask mockTask = null
}

// Initialise graphs and fields that the code samples will use

uri = JavaDocsTest.engine.uri()
host = "localhost"
port = JavaDocsTest.engine.port()

tx = DocTestUtil.getTestGraph(uri).open(GraknTxType.WRITE)

_otherTx = DocTestUtil.getTestGraph(uri).open(GraknTxType.WRITE)
keyspace = _otherTx.getKeyspace()
_otherTx.close()

callback = {x -> x}

qb = tx.graql()

def body() {
$putTheBodyHere
}

try {
    body()
} finally {
    tx.close()
}



