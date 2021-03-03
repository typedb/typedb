/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.test.integration;

import grakn.core.Grakn;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options.Database;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.rocks.RocksGrakn;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static grakn.core.test.integration.util.Util.assertNotNulls;
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
    private static String database = "query-test";

    @Test
    public void test_query_define() throws IOException {
        Util.resetDirectory(dataDir);

        try (Grakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
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

        try (Grakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String queryString = "undefine analysis abstract, owns created, plays commit-analysis:analysis;";
                    GraqlUndefine query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine rule performance-tracker-rule;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine performance-tracker relates tracker;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine email regex '.+\\@.+\\..+';";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    queryString = "undefine index sub attribute, value long;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    // undefine first 4 rules
                    queryString = "undefine rule repo-fork-rule;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);
                    queryString = "undefine rule repo-dependency-transitive-rule;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);
                    queryString = "undefine rule repo-dependency-transitive-type-rule;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);
                    queryString = "undefine rule repo-collaborator-org-rule;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
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
                    List<ConceptMap> answers = tx.query().match(Graql.parseQuery("match $x isa repo-fork;").asMatch()).toList();
                }
            }
        }
    }

    @Test
    public void test_query_insert() throws IOException {
        Util.resetDirectory(dataDir);

        try (Grakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }
            }

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String queryString = "insert " +
                            "$n 'graknlabs' isa name; " +
                            "$o isa organisation, has name $n; " +
                            "$t isa team, has name 'engineers', has symbol 'graknlabs/engineers'; " +
                            "$u isa user, has name 'grabl', has email 'grabl@grakn.ai'; " +
                            "($o, $t) isa org-team; " +
                            "($o, $u) isa org-member; " +
                            "($t, $u) isa team-member;";

                    GraqlInsert query = Graql.parseQuery(queryString);
                    transaction.query().insert(query);

                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    Attribute.String name_graknlabs = transaction.concepts().getAttributeType("name").asString().get("graknlabs");
                    Attribute.String symbol_engineers = transaction.concepts().getAttributeType("symbol").asString().get("graknlabs/engineers");
                    Attribute.String email_grabl = transaction.concepts().getAttributeType("email").asString().get("grabl@grakn.ai");
                    assertNotNulls(name_graknlabs, symbol_engineers, email_grabl);

                    Entity organisation_graknlabs = name_graknlabs.getOwners().findAny().get().asEntity();
                    Entity team_engineers = symbol_engineers.getOwners().findAny().get().asEntity();
                    Entity user_grabl = email_grabl.getOwners().findAny().get().asEntity();
                    assertNotNulls(organisation_graknlabs, team_engineers, user_grabl);

                    assertEquals(organisation_graknlabs.getRelations("org-team:org").findAny().get().getPlayers("team").findAny().get(), team_engineers);
                    assertEquals(organisation_graknlabs.getRelations("org-member:org").findAny().get().getPlayers("member").findAny().get(), user_grabl);
                    assertEquals(team_engineers.getRelations("team-member:team").findAny().get().getPlayers("member").findAny().get(), user_grabl);
                }
            }
        }
    }

    @Test
    public void test_query_delete() throws IOException {
        Util.resetDirectory(dataDir);

        try (Grakn grakn = RocksGrakn.open(options)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }
            }

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String insertString = "insert " +
                            "$o isa organisation, has name 'graknlabs'; " +
                            "$t isa team, has name 'engineers', has symbol 'graknlabs/engineers'; " +
                            "$u isa user, has name 'grabl', has email 'grabl@grakn.ai'; " +
                            "($o, $t) isa org-team; " +
                            "($o, $u) isa org-member; " +
                            "($t, $u) isa team-member;";
                    GraqlInsert insertQuery = Graql.parseQuery(insertString);
                    transaction.query().insert(insertQuery);
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String deleteString = "match $x isa thing; delete $x isa thing;";
                    GraqlDelete deleteQuery = Graql.parseQuery(deleteString);
                    transaction.query().delete(deleteQuery);
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    String matchString = "match $x isa thing;";
                    GraqlMatch matchQuery = Graql.parseQuery(matchString);
                    FunctionalIterator<ConceptMap> answers = transaction.query().match(matchQuery);
                    assertFalse(answers.hasNext());
                }
            }
        }
    }
}
