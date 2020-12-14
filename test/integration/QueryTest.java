/*
 * Copyright (C) 2020 Grakn Labs
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
import grakn.core.common.parameters.Arguments;
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
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlUndefine;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.test.integration.util.Util.assertNotNulls;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class QueryTest {

    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("query-test");
    private static String database = "query-test";

    @Test
    public void test_query_define() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    final AttributeType.String name = tx.concepts().getAttributeType("name").asString();
                    final AttributeType.String symbol = tx.concepts().getAttributeType("symbol").asString();
                    final AttributeType.Boolean active = tx.concepts().getAttributeType("active").asBoolean();
                    final AttributeType.Long priority = tx.concepts().getAttributeType("priority").asLong();
                    assertNotNulls(name, symbol, active, priority);

                    final EntityType organisation = tx.concepts().getEntityType("organisation");
                    final EntityType team = tx.concepts().getEntityType("team");
                    final EntityType user = tx.concepts().getEntityType("user");
                    final EntityType repository = tx.concepts().getEntityType("repository");
                    final EntityType branchRule = tx.concepts().getEntityType("branch-rule");
                    final EntityType commit = tx.concepts().getEntityType("commit");
                    assertNotNulls(organisation, team, user, repository, branchRule, commit);

                    assertTrue(organisation.getOwns().anyMatch(a -> a.equals(name)));
                    assertTrue(team.getOwns().anyMatch(a -> a.equals(symbol)));
                    assertTrue(user.getOwns().anyMatch(a -> a.equals(name)));
                    assertTrue(repository.getOwns().anyMatch(a -> a.equals(active)));
                    assertTrue(branchRule.getOwns().anyMatch(a -> a.equals(priority)));
                    assertTrue(commit.getOwns().anyMatch(a -> a.equals(symbol)));

                    final RelationType orgTeam = tx.concepts().getRelationType("org-team");
                    final RelationType teamMember = tx.concepts().getRelationType("team-member");
                    final RelationType repoDependency = tx.concepts().getRelationType("repo-dependency");
                    assertNotNulls(orgTeam, teamMember, repoDependency);

                    final RoleType orgTeam_org = orgTeam.getRelates("org");
                    final RoleType orgTeam_team = orgTeam.getRelates("team");
                    final RoleType teamMember_team = teamMember.getRelates("team");
                    final RoleType teamMember_member = teamMember.getRelates("member");
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
                    assertEquals(20, tx.logic().rules().toList().size());
                }
            }
        }
    }

    @Test
    public void test_query_undefine() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    String queryString = "undefine analysis abstract, owns created, plays commit-analysis:analysis;";
                    GraqlUndefine query = Graql.parseQuery(queryString);
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

                    // undefine `performance-tracker-rule` because it depends on an undefined role performance-tracker:tracker
                    // else the commit would throw
                    queryString = "undefine rule performance-tracker-rule;";
                    query = Graql.parseQuery(queryString);
                    transaction.query().undefine(query);

                    transaction.commit();
                }

                try (Grakn.Transaction tx = session.transaction(Arguments.Transaction.Type.READ)) {
                    final EntityType analysis = tx.concepts().getEntityType("analysis");
                    final RelationType performanceTracker = tx.concepts().getRelationType("performance-tracker");
                    final RoleType commitAnalysisAnalysis = tx.concepts().getRelationType("commit-analysis").getRelates("analysis");
                    final AttributeType.DateTime created = tx.concepts().getAttributeType("created").asDateTime();
                    final AttributeType.String email = tx.concepts().getAttributeType("email").asString();
                    assertNotNulls(analysis, performanceTracker, commitAnalysisAnalysis, created, email);

                    assertFalse(analysis.isAbstract());
                    assertTrue(analysis.getOwns().noneMatch(att -> att.equals(created)));
                    assertTrue(analysis.getPlays().noneMatch(rol -> rol.equals(commitAnalysisAnalysis)));
                    assertTrue(performanceTracker.getRelates().noneMatch(rol -> rol.getLabel().name().equals("tracker")));
                    assertNull(email.getRegex());

                    final AttributeType index = tx.concepts().getAttributeType("index");
                    assertNull(index);

                    assertNull(tx.logic().getRule("repo-fork-rule"));
                    assertNull(tx.logic().getRule("repo-dependency-transitive-rule"));
                    assertNull(tx.logic().getRule("repo-dependency-transitive-type-rule"));
                    assertNull(tx.logic().getRule("repo-collaborator-org-rule"));

                    // check total count
                    assertEquals(20 - 5, tx.logic().rules().toList().size());
                }
            }
        }
    }

    @Test
    public void test_query_insert() throws IOException {
        Util.resetDirectory(directory);

        try (Grakn grakn = RocksGrakn.open(directory)) {
            grakn.databases().create(database);

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final GraqlDefine query = Graql.parseQuery(new String(Files.readAllBytes(Paths.get("test/integration/schema.gql")), UTF_8));
                    transaction.query().define(query);
                    transaction.commit();
                }
            }

            try (Grakn.Session session = grakn.session(database, Arguments.Session.Type.DATA)) {
                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.WRITE)) {
                    final String queryString = "insert " +
                            "$n 'graknlabs' isa name; " +
                            "$o isa organisation, has name $n; " +
                            "$t isa team, has name 'engineers', has symbol 'graknlabs/engineers'; " +
                            "$u isa user, has name 'grabl', has email 'grabl@grakn.ai'; " +
                            "($o, $t) isa org-team; " +
                            "($o, $u) isa org-member; " +
                            "($t, $u) isa team-member;";

                    final GraqlInsert query = Graql.parseQuery(queryString);
                    transaction.query().insert(query);

                    transaction.commit();
                }

                try (Grakn.Transaction transaction = session.transaction(Arguments.Transaction.Type.READ)) {
                    final Attribute.String name_graknlabs = transaction.concepts().getAttributeType("name").asString().get("graknlabs");
                    final Attribute.String symbol_engineers = transaction.concepts().getAttributeType("symbol").asString().get("graknlabs/engineers");
                    final Attribute.String email_grabl = transaction.concepts().getAttributeType("email").asString().get("grabl@grakn.ai");
                    assertNotNulls(name_graknlabs, symbol_engineers, email_grabl);

                    final Entity organisation_graknlabs = name_graknlabs.getOwners().findAny().get().asEntity();
                    final Entity team_engineers = symbol_engineers.getOwners().findAny().get().asEntity();
                    final Entity user_grabl = email_grabl.getOwners().findAny().get().asEntity();
                    assertNotNulls(organisation_graknlabs, team_engineers, user_grabl);

                    assertEquals(organisation_graknlabs.getRelations("org-team:org").findAny().get().getPlayers("team").findAny().get(), team_engineers);
                    assertEquals(organisation_graknlabs.getRelations("org-member:org").findAny().get().getPlayers("member").findAny().get(), user_grabl);
                    assertEquals(team_engineers.getRelations("team-member:team").findAny().get().getPlayers("member").findAny().get(), user_grabl);
                }
            }
        }
    }
}
