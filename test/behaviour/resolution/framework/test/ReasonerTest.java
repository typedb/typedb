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

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.Reasoner;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ReasonerTest {

    private static final String database = "ReasonerTest";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).logsDir(logDir);
    private RocksTypeDB typeDB;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.typeDB = RocksTypeDB.open(options);
        this.typeDB.databases().create(database);
    }

    @After
    public void tearDown() {
        this.typeDB.close();
    }

    @Test
    public void testDeduplicationOfInferredConcepts() {
        loadTransitivityExample(typeDB);
        try (RocksSession session = typeDB.session(database, Arguments.Session.Type.DATA)) {
            Reasoner referenceReasoner = Reasoner.runRules(session);
            TypeQLMatch inferredAnswersQuery = TypeQL.match(TypeQL.var("lh").isa("location-hierarchy"));
            List<ConceptMap> inferredAnswers = referenceReasoner.tx().query().match(inferredAnswersQuery).toList();
            assertEquals(6, inferredAnswers.size());
        }
    }

    // TODO: These should use TypeQL builder to make them robust to change
    static void loadTransitivityExample(TypeDB typeDB) {
        try (TypeDB.Session session = typeDB.session(ReasonerTest.database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                String schema = "" +
                        "define\n" +
                        "name sub attribute,\n" +
                        "    value string;\n" +
                        "location sub entity,\n" +
                        "    abstract,\n" +
                        "    owns name @key,\n" +
                        "    plays location-hierarchy:superior,\n" +
                        "    plays location-hierarchy:subordinate;\n" +
                        "area sub location;\n" +
                        "city sub location;\n" +
                        "country sub location;\n" +
                        "continent sub location;\n" +
                        "location-hierarchy sub relation,\n" +
                        "    relates superior,\n" +
                        "    relates subordinate;\n" +
                        "rule location-hierarchy-transitivity:\n" +
                        "when {\n" +
                        "    (superior: $a, subordinate: $b) isa location-hierarchy;\n" +
                        "    (superior: $b, subordinate: $c) isa location-hierarchy;\n" +
                        "} then {\n" +
                        "    (superior: $a, subordinate: $c) isa location-hierarchy;\n" +
                        "};";
                tx.query().define(TypeQL.parseQuery(schema).asDefine());
                tx.commit();
            }
        }
        try (TypeDB.Session session = typeDB.session(ReasonerTest.database, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.parseQuery(
                        "insert\n" +
                                "$ar isa area, has name \"King's Cross\";\n" +
                                "$cit isa city, has name \"London\";\n" +
                                "$cntry isa country, has name \"UK\";\n" +
                                "$cont isa continent, has name \"Europe\";\n" +
                                "(superior: $cont, subordinate: $cntry) isa location-hierarchy;\n" +
                                "(superior: $cntry, subordinate: $cit) isa location-hierarchy;\n" +
                                "(superior: $cit, subordinate: $ar) isa location-hierarchy;"
                ).asInsert());
                tx.commit();
            }
        }
    }
}