/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.reasoner.verification.test;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreSession;
import com.vaticle.typedb.core.test.behaviour.reasoner.verification.ForwardChainingMaterialiser;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLGet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.TypeQL.and;
import static com.vaticle.typeql.lang.TypeQL.define;
import static com.vaticle.typeql.lang.TypeQL.rule;
import static com.vaticle.typeql.lang.TypeQL.type;
import static com.vaticle.typeql.lang.TypeQL.cVar;
import static com.vaticle.typeql.lang.common.TypeQLArg.ValueType.STRING;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ATTRIBUTE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ENTITY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.RELATION;
import static org.junit.Assert.assertEquals;

public class MaterialiserTest {

    private static final String database = "MaterialiserTest";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve(database);
    private static final Path logDir = dataDir.resolve("logs");
    private static final Options.Database options = new Options.Database().dataDir(dataDir).reasonerDebuggerDir(logDir)
            .storageIndexCacheSize(MB).storageDataCacheSize(MB);
    private CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        Util.resetDirectory(dataDir);
        this.databaseMgr = CoreDatabaseManager.open(options);
        this.databaseMgr.create(database);
    }

    @After
    public void tearDown() {
        this.databaseMgr.close();
    }

    @Test
    public void testDeduplicationOfInferredConcepts() {
        loadTransitivityExample(databaseMgr);
        try (CoreSession session = databaseMgr.session(database, Arguments.Session.Type.DATA)) {
            ForwardChainingMaterialiser materialiser = ForwardChainingMaterialiser.materialise(session);
            TypeQLGet inferredAnswersQuery = TypeQL.match(TypeQL.cVar("lh").isa("location-hierarchy")).get();
            List<ConceptMap> inferredAnswers = iterate(materialiser.query(inferredAnswersQuery).entrySet())
                    .flatMap(Map.Entry::getValue).toList();
            assertEquals(6, inferredAnswers.size());
        }
    }

    private static void loadTransitivityExample(TypeDB.DatabaseManager typedb) {
        try (TypeDB.Session session = typedb.session(MaterialiserTest.database, Arguments.Session.Type.SCHEMA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(define(list(
                        type("name").sub(ATTRIBUTE).value(STRING),
                        type("location").sub(ENTITY)
                                .isAbstract()
                                .owns("name", KEY)
                                .plays("location-hierarchy", "superior")
                                .plays("location-hierarchy", "subordinate"),
                        type("area").sub("location"),
                        type("city").sub("location"),
                        type("country").sub("location"),
                        type("continent").sub("location"),
                        type("location-hierarchy").sub(RELATION)
                                .relates("superior")
                                .relates("subordinate"),
                        rule("location-hierarchy-transitivity")
                                .when(and(
                                        cVar().rel("superior", cVar("a")).rel("subordinate", cVar("b")).isa("location-hierarchy"),
                                        cVar().rel("superior", cVar("b")).rel("subordinate", cVar("c")).isa("location-hierarchy")
                                )).then(cVar().rel("superior", cVar("a")).rel("subordinate", cVar("c")).isa("location-hierarchy"))
                )));
                tx.commit();
            }
        }
        try (TypeDB.Session session = typedb.session(MaterialiserTest.database, Arguments.Session.Type.DATA)) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().insert(TypeQL.insert(list(
                        cVar("area").isa("area").has("name", "King's Cross"),
                        cVar("city").isa("city").has("name", "London"),
                        cVar("country").isa("country").has("name", "UK"),
                        cVar("continent").isa("continent").has("name", "Europe"),
                        cVar().rel("superior", cVar("continent")).rel("subordinate", cVar("country")).isa("location-hierarchy"),
                        cVar().rel("superior", cVar("country")).rel("subordinate", cVar("city")).isa("location-hierarchy"),
                        cVar().rel("superior", cVar("city")).rel("subordinate", cVar("area")).isa("location-hierarchy")
                )).asInsert());
                tx.commit();
            }
        }
    }
}
