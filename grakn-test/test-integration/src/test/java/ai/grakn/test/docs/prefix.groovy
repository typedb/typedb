/*-
 * #%L
 * test-integration
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
 */

package ai.grakn.test.docs

import ai.grakn.*
import ai.grakn.client.BatchExecutorClient
import ai.grakn.concept.*
import ai.grakn.engine.TaskId
import ai.grakn.graql.*
import ai.grakn.graql.admin.Answer
import ai.grakn.migration.csv.CSVMigrator
import mjson.Json

import java.time.LocalDateTime

import static ai.grakn.concept.AttributeType.DataType.STRING
import static ai.grakn.graql.Graql.*

// This is some dumb stuff so IntelliJ doesn't get rid of the imports
//noinspection GroovyConstantIfStatement
if (false) {
    label("hello")
    Answer answer = null
    Concept concept = null
    Var var = var()
    GraknSession session = null
    LocalDateTime time = null
    CSVMigrator migrator = null
    BatchExecutorClient client = null
    Json json = null
    TaskId id = null
    str = STRING
    Label label = null
    Order order = null
    GraknTx tx = null
    Attribute attribute = null
}

// Initialise graphs and fields that the code samples will use

uri = JavaDocsTest.engine.uri()
host = uri.host
port = uri.port

tx = DocTestUtil.getTestGraph(uri, JavaDocsTest.knowledgeBaseName).open(GraknTxType.WRITE)

_otherTx = DocTestUtil.getTestGraph(uri, JavaDocsTest.knowledgeBaseName).open(GraknTxType.WRITE)
keyspace = _otherTx.keyspace()
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



