/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.benchmark;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.schema.Rule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static org.junit.Assert.assertEquals;

public class RuleScaling {

    private final String database = "rule-scaling-test";
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("rule-scaling-test");
    private static final Options.Database options = new Options.Database().dataDir(dataDir)
            .storageDataCacheSize(MB).storageIndexCacheSize(MB);

    private static CoreDatabaseManager databaseMgr;

    @Before
    public void setUp() throws IOException {
        com.vaticle.typedb.core.test.integration.util.Util.resetDirectory(dataDir);
        databaseMgr = CoreDatabaseManager.open(options);
        databaseMgr.create(database);
    }

    @After
    public void tearDown() {
        databaseMgr.close();
    }

    private TypeDB.Session schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private TypeDB.Session dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    @Test
    public void ruleScaling() {
        final int N = 60;
        final int populatedChains = 3;

        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                tx.query().define(TypeQL.parseQuery(
                        "define " +
                                "rootRelation sub relation, relates someRole, relates anotherRole; " + // Base class for convenience
                                "baseEntity sub entity, plays rootRelation:someRole, plays rootRelation:anotherRole;" +
                                "someEntity sub baseEntity;" +
                                "intermediateEntity sub baseEntity;" +
                                "finalEntity sub baseEntity;" +
                                "baseRelation sub rootRelation;" +
                                "anotherBaseRelation sub rootRelation;" +
                                "inferredRelation sub rootRelation, " +
                                "   owns inferredAttribute, owns anotherInferredAttribute;" +
                                "indexingRelation sub rootRelation;" +
                                "inferredAttribute sub attribute, value string;" +
                                "anotherInferredAttribute sub attribute, value string;"
                ).asDefine());

                for (int i = 0; i < N; i++) {
                    tx.query().define(TypeQL.parseQuery(
                            "define " +
                                    "specificRelation" + i + " sub rootRelation;" +
                                    "anotherSpecificRelation" + i + " sub rootRelation;"
                    ).asDefine());
                }
                tx.commit();
            }
        }

        String basePattern =
                "$x isa someEntity;" +
                        "(someRole: $x, anotherRole: $y) isa baseRelation;" +
                        "(someRole: $y, anotherRole: $link) isa anotherBaseRelation;";

        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {

                for (int i = 0; i < N; i++) {
                    Pattern specificPattern = TypeQL.parsePattern(
                            "{" +
                                    basePattern +
                                    "(someRole: $link, anotherRole: $index) isa specificRelation" + i + ";" +
                                    "(someRole: $index, anotherRole: $anotherLink) isa anotherSpecificRelation" + i + ";" +
                                    "}"
                    );
                    Rule relationRule = TypeQL.rule("relationRule" + i)
                            .when(specificPattern.asConjunction())
                            .then(TypeQL.parseVariable("(someRole: $link, anotherRole: $anotherLink) isa inferredRelation").asThing());

                    tx.query().define(TypeQL.define(relationRule));

                    Rule attributeRule = TypeQL.rule("attributeRule" + i)
                            .when(TypeQL.and(
                                    specificPattern,
                                    TypeQL.parsePattern("$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation")
                            ))
                            .then(TypeQL.parseVariable("$r has inferredAttribute 'inferredValue" + i + "'").asThing());

                    tx.query().define(TypeQL.define(attributeRule));

                    Rule anotherAttributeRule = TypeQL.rule("anotherAttributeRule" + i)
                            .when(TypeQL.and(
                                    specificPattern,
                                    TypeQL.parsePattern("$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation"),
                                    TypeQL.parsePattern("$r has inferredAttribute 'inferredValue" + i + "'")
                            ))
                            .then(TypeQL.parseVariable("$r has anotherInferredAttribute 'anotherInferredValue" + i + "'").asThing());

                    tx.query().define(TypeQL.define(anotherAttributeRule));

                    Rule indexingRelationRule = TypeQL.rule("indexingRelationRule" + i)
                            .when(TypeQL.and(specificPattern,
                                    TypeQL.parsePattern("$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation"),
                                    TypeQL.parsePattern("$r has inferredAttribute 'inferredValue" + i + "'"),
                                    TypeQL.parsePattern("$r has anotherInferredAttribute 'anotherInferredValue" + i + "'")))
                            .then(TypeQL.parseVariable("(someRole: $anotherLink, anotherRole: $index) isa indexingRelation").asThing());

                    tx.query().define(TypeQL.define(indexingRelationRule));
                }
                tx.commit();
            }
        }

        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                for (int k = 0; k < populatedChains; k++) {
                    tx.query().insert(TypeQL.parseQuery(
                            "insert " +
                                    "$x isa someEntity;" +
                                    "$y isa intermediateEntity;" +
                                    "$link isa intermediateEntity;" +
                                    "$index isa intermediateEntity;" +
                                    "$anotherLink isa finalEntity;" +
                                    "(someRole: $x, anotherRole: $y) isa baseRelation;" +
                                    "(someRole: $y, anotherRole: $link) isa anotherBaseRelation;" +
                                    "(someRole: $link, anotherRole: $index) isa specificRelation" + k + ";" +
                                    "(someRole: $index, anotherRole: $anotherLink) isa anotherSpecificRelation" + k + ";"
                    ).asInsert());
                }
                tx.commit();
            }

            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                String query = "match " +
                        "$x isa someEntity;" +
                        "(someRole: $x, anotherRole: $y) isa baseRelation;" +
                        "(someRole: $y, anotherRole: $link) isa anotherBaseRelation;" +
                        "$r (someRole: $link, anotherRole: $anotherLink) isa inferredRelation;" +
                        "$r has inferredAttribute $value;" +
                        "$r has anotherInferredAttribute $anotherValue;" +
                        "(someRole: $anotherLink, anotherRole: $index) isa indexingRelation;";
                List<ConceptMap> answers = Util.timeQuery(query, tx, "RuleScaling query");
                assertEquals(populatedChains, answers.size());
            }
        }
    }
}
