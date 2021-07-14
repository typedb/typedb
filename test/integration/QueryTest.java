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

package com.vaticle.typedb.core.test.integration;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLUndefine;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.vaticle.typedb.core.test.integration.util.Util.assertNotNulls;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class QueryTest {

    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).logsDir(logDir);
    private static final String database = "query-test";

    @Test
    public void test_query_define() throws IOException {
        Util.resetDirectory(dataDir);

        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {

                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    TypeQLDefine query = TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    AttributeType.String name = tx.concepts().getAttributeType("name").asString();
                    AttributeType.String symbol = tx.concepts().getAttributeType("symbol").asString();
                    AttributeType.Boolean active = tx.concepts().getAttributeType("active").asBoolean();
                    AttributeType.Long priority = tx.concepts().getAttributeType("priority").asLong();
                    assertNotNulls(name, symbol, active, priority);

                    EntityType organisation = tx.concepts().getEntityType("organisation");
                    EntityType team = tx.concepts().getEntityType("team");
                    EntityType user = tx.concepts().getEntityType("user");
                    EntityType repository = tx.concepts().getEntityType("repository");
                    EntityType branchRule = tx.concepts().getEntityType("branch-rule");
                    EntityType commit = tx.concepts().getEntityType("commit");
                    assertNotNulls(organisation, team, user, repository, branchRule, commit);

                    assertTrue(organisation.getOwns().anyMatch(a -> a.equals(name)));
                    assertTrue(team.getOwns().anyMatch(a -> a.equals(symbol)));
                    assertTrue(user.getOwns().anyMatch(a -> a.equals(name)));
                    assertTrue(repository.getOwns().anyMatch(a -> a.equals(active)));
                    assertTrue(branchRule.getOwns().anyMatch(a -> a.equals(priority)));
                    assertTrue(commit.getOwns().anyMatch(a -> a.equals(symbol)));

                    RelationType orgTeam = tx.concepts().getRelationType("org-team");
                    RelationType teamMember = tx.concepts().getRelationType("team-member");
                    RelationType repoDependency = tx.concepts().getRelationType("repo-dependency");
                    assertNotNulls(orgTeam, teamMember, repoDependency);

                    RoleType orgTeam_org = orgTeam.getRelates("org");
                    RoleType orgTeam_team = orgTeam.getRelates("team");
                    RoleType teamMember_team = teamMember.getRelates("team");
                    RoleType teamMember_member = teamMember.getRelates("member");
                    assertNotNulls(orgTeam_org, orgTeam_team, teamMember_team, teamMember_member);

                    assertTrue(organisation.getPlays().anyMatch(r -> r.equals(orgTeam_org)));
                    assertTrue(team.getPlays().anyMatch(r -> r.equals(orgTeam_team)));
                    assertTrue(team.getPlays().anyMatch(r -> r.equals(teamMember_team)));
                    assertTrue(user.getPlays().anyMatch(r -> r.equals(teamMember_member)));

                    // check first 4 rules
                    assertNotNull(tx.logic().getRule("repo-fork-rule"));
                    assertNotNull(tx.logic().getRule("repo-dependency-transitive-rule"));
                    assertNotNull(tx.logic().getRule("repo-dependency-transitive-type-rule"));
                    assertNotNull(tx.logic().getRule("repo-collaborator-org-rule"));
                    // check total count
                    assertEquals(15, tx.logic().rules().toList().size());
                }
            }
        }
    }

    @Test
    public void test_query_undefine() throws IOException {
        Util.resetDirectory(dataDir);

        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {

                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    TypeQLDefine query = TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String queryString = "undefine analysis abstract, owns created, plays commit-analysis:analysis;";
                    TypeQLUndefine query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine rule performance-tracker-rule;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine performance-tracker relates tracker;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine email regex '.+\\@.+\\..+';";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine index sub attribute, value long;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);

                    // undefine first 4 rules
                    queryString = "undefine rule repo-fork-rule;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);
                    queryString = "undefine rule repo-dependency-transitive-rule;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);
                    queryString = "undefine rule repo-dependency-transitive-type-rule;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);
                    queryString = "undefine rule repo-collaborator-org-rule;";
                    query = TypeQL.parseQuery(queryString);
                    transaction.query().undefine(query);

                    transaction.commit();
                }

                try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    EntityType analysis = tx.concepts().getEntityType("analysis");
                    RelationType performanceTracker = tx.concepts().getRelationType("performance-tracker");
                    RoleType commitAnalysisAnalysis = tx.concepts().getRelationType("commit-analysis").getRelates("analysis");
                    AttributeType.DateTime created = tx.concepts().getAttributeType("created").asDateTime();
                    AttributeType.String email = tx.concepts().getAttributeType("email").asString();
                    assertNotNulls(analysis, performanceTracker, commitAnalysisAnalysis, created, email);

                    assertFalse(analysis.isAbstract());
                    assertTrue(analysis.getOwns().noneMatch(att -> att.equals(created)));
                    assertTrue(analysis.getPlays().noneMatch(rol -> rol.equals(commitAnalysisAnalysis)));
                    assertTrue(performanceTracker.getRelates().noneMatch(rol -> rol.getLabel().name().equals("tracker")));
                    assertNull(email.getRegex());

                    AttributeType index = tx.concepts().getAttributeType("index");
                    assertNull(index);

                    assertNull(tx.logic().getRule("repo-fork-rule"));
                    assertNull(tx.logic().getRule("repo-dependency-transitive-rule"));
                    assertNull(tx.logic().getRule("repo-dependency-transitive-type-rule"));
                    assertNull(tx.logic().getRule("repo-collaborator-org-rule"));

                    // check total count
                    assertEquals(15 - 5, tx.logic().rules().toList().size());

                    // a query that used to trigger a rule should not cause an error
                    List<ConceptMap> answers = tx.query().match(TypeQL.parseQuery("match $x isa repo-fork;").asMatch()).toList();
                }
            }
        }
    }

    @Test
    public void test_query_insert() throws IOException {
        Util.resetDirectory(dataDir);

        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    TypeQLDefine query = TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }
            }

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String queryString = "insert " +
                            "$n 'vaticle' isa name; " +
                            "$o isa organisation, has name $n; " +
                            "$t isa team, has name 'engineers', has symbol 'vaticle/engineers'; " +
                            "$u isa user, has name 'butler', has email 'butler@vaticle.com'; " +
                            "($o, $t) isa org-team; " +
                            "($o, $u) isa org-member; " +
                            "($t, $u) isa team-member;";

                    TypeQLInsert query = TypeQL.parseQuery(queryString);
                    transaction.query().insert(query);

                    transaction.commit();
                }

                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    Attribute.String name_vaticle = transaction.concepts().getAttributeType("name").asString().get("vaticle");
                    Attribute.String symbol_engineers = transaction.concepts().getAttributeType("symbol").asString().get("vaticle/engineers");
                    Attribute.String email_butler = transaction.concepts().getAttributeType("email").asString().get("butler@vaticle.com");
                    assertNotNulls(name_vaticle, symbol_engineers, email_butler);

                    Entity organisation_vaticle = name_vaticle.getOwners().first().get().asEntity();
                    Entity team_engineers = symbol_engineers.getOwners().first().get().asEntity();
                    Entity user_butler = email_butler.getOwners().first().get().asEntity();
                    assertNotNulls(organisation_vaticle, team_engineers, user_butler);

                    assertEquals(organisation_vaticle.getRelations("org-team:org").first().get().getPlayers("team").first().get(), team_engineers);
                    assertEquals(organisation_vaticle.getRelations("org-member:org").first().get().getPlayers("member").first().get(), user_butler);
                    assertEquals(team_engineers.getRelations("team-member:team").first().get().getPlayers("member").first().get(), user_butler);
                }
            }
        }
    }

    @Test
    public void test_query_delete() throws IOException {
        Util.resetDirectory(dataDir);

        try (TypeDB typedb = RocksTypeDB.open(options)) {
            typedb.databases().create(database);

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.SCHEMA)) {
                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    TypeQLDefine query = TypeQL.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }
            }

            try (TypeDB.Session session = typedb.session(database, Arguments.Session.Type.DATA)) {
                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String insertString = "insert " +
                            "$o isa organisation, has name 'vaticle'; " +
                            "$t isa team, has name 'engineers', has symbol 'vaticle/engineers'; " +
                            "$u isa user, has name 'butler', has email 'butler@vaticle.com'; " +
                            "($o, $t) isa org-team; " +
                            "($o, $u) isa org-member; " +
                            "($t, $u) isa team-member;";
                    TypeQLInsert insertQuery = TypeQL.parseQuery(insertString);
                    transaction.query().insert(insertQuery);
                    transaction.commit();
                }

                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String deleteString = "match $x isa thing; delete $x isa thing;";
                    TypeQLDelete deleteQuery = TypeQL.parseQuery(deleteString);
                    transaction.query().delete(deleteQuery);
                    transaction.commit();
                }

                try (TypeDB.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    String matchString = "match $x isa thing;";
                    TypeQLMatch matchQuery = TypeQL.parseQuery(matchString);
                    FunctionalIterator<ConceptMap> answers = transaction.query().match(matchQuery);
                    assertFalse(answers.hasNext());
                }
            }
        }
    }
}
