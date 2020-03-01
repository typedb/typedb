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
 */

package grakn.core.graql.reasoner.reasoning;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class TypeHierarchiesIT {

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Test //Expected result: The query should not return any matches (or possibly return a single match with $x=$y)
    public void roleUnificationWithRoleHierarchiesInvolved() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet8.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String queryString = "match (role2:$x, role3:$y) isa relation2; get;";
                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                assertThat(answers, empty());
            }
        }
    }

    @Test //Expected result: The query should return a unique match
    public void rulesInteractingWithTypeHierarchy() {
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet13.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String queryString = "match (role1:$x, role2:$y) isa relation2; get;";
                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                assertEquals(1, answers.size());
            }
        }
    }

    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet19.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseTypeQuery = "match " +
                        "$x isa entity1;" +
                        "$y isa entity1;" +
                        "(role1: $x, role2: $y) isa relation1;";
                String baseTypeWithBoundQuery = baseTypeQuery + "$y has name 'a';";
                List<ConceptMap> baseTypes = tx.execute(Graql.parse(baseTypeQuery + " get;").asGet());
                assertEquals(2, baseTypes.size());
                List<ConceptMap> specificBaseTypes = tx.execute(Graql.parse(baseTypeWithBoundQuery + " get;").asGet());
                assertEquals(2, specificBaseTypes.size());

                String specialisedTypeQuery = "match " +
                        "$x isa entity1;" +
                        "$y isa subEntity1;" +
                        "(role1: $x, role2: $y) isa relation1;";
                String specialisedTypeWithBoundQuery = specialisedTypeQuery + "$y has name 'a';";
                List<ConceptMap> specialisedTypes = tx.execute(Graql.parse(specialisedTypeQuery + " get;").asGet());
                assertEquals(1, specialisedTypes.size());
                List<ConceptMap> specificSpecialisedInstances = tx.execute(Graql.parse(specialisedTypeWithBoundQuery + " get;").asGet());
                assertEquals(1, specificSpecialisedInstances.size());

                String overwrittenTypeQuery = "match " +
                        "$x isa subEntity1;" +
                        "$y isa entity1;" +
                        "(role1: $x, role2: $y) isa relation1;";
                String overwrittenTypeWithBoundQuery = overwrittenTypeQuery + "$y has name 'a';";
                List<ConceptMap> overwrittenTypes = tx.execute(Graql.parse(overwrittenTypeQuery + "get;").asGet());
                assertEquals(2, overwrittenTypes.size());
                List<ConceptMap> specificInstancesWithTypeOverwrite = tx.execute(Graql.parse(overwrittenTypeWithBoundQuery + "get;").asGet());
                assertEquals(2, specificInstancesWithTypeOverwrite.size());
            }
        }
    }


    @Test //Expected result: Two answers obtained only if the rule query containing sub type is correctly executed.
    public void instanceTypeHierarchyRespected_queryHasSuperTypes_recursiveRule() {
        try (Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet19-recursive.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String baseTypeQuery = "match " +
                        "$x isa entity1;" +
                        "$y isa entity1;" +
                        "(role1: $x, role2: $y) isa relation1;";
                String boundedBaseTypeQuery = baseTypeQuery + "$y has name 'a';";
                List<ConceptMap> baseRPs = tx.execute(Graql.parse(baseTypeQuery + " get;").asGet());
                assertEquals(2, baseRPs.size());
                List<ConceptMap> specificBaseRPs = tx.execute(Graql.parse(boundedBaseTypeQuery + " get;").asGet());
                assertEquals(2, specificBaseRPs.size());

                String specialisedTypeQuery = "match " +
                        "$x isa entity1;" +
                        "$y isa subEntity1;" +
                        "(role1: $x, role2: $y) isa relation1;";
                String specialisedTypeWithBoundQuery = specialisedTypeQuery + "$y has name 'a';";

                List<ConceptMap> specialisedRPs = tx.execute(Graql.parse(specialisedTypeQuery + " get;").asGet());
                assertEquals(1, specialisedRPs.size());
                List<ConceptMap> specificSpecialisedRPs = tx.execute(Graql.parse(specialisedTypeWithBoundQuery + " get;").asGet());
                assertEquals(1, specificSpecialisedRPs.size());

                String typeOverwriteQuery = "match " +
                        "$x isa subEntity1;" +
                        "$y isa entity1;" +
                        "(role1: $x, role2: $y) isa relation1;";
                String boundedTypeOverwriteQuery = typeOverwriteQuery + "$y has name 'a';";

                List<ConceptMap> typeOverwriteRps = tx.execute(Graql.parse(typeOverwriteQuery + " get;").asGet());
                assertEquals(2, typeOverwriteRps.size());
                List<ConceptMap> specificTypeOverwriteRPs = tx.execute(Graql.parse(boundedTypeOverwriteQuery + " get;").asGet());
                assertEquals(2, specificTypeOverwriteRPs.size());
            }
        }
    }


    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverRelationHierarchy(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet20.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String queryString = "match (role1: $x, role2: $y) isa relation1; get;";
                String queryString2 = "match (role1: $x, role2: $y) isa sub-relation1; get;";
                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
                assertEquals(1, answers.size());
                assertTrue(answers.containsAll(answers2));
                assertTrue(answers2.containsAll(answers));
            }
        }
    }

    @Test //Expected result: Both queries should return a single equal match as they trigger the same rule.
    public void reasoningOverEntityHierarchy(){
        try(Session session = server.sessionWithNewKeyspace()) {
            loadFromFileAndCommit(resourcePath, "testSet21.gql", session);
            try (Transaction tx = session.writeTransaction()) {
                                String queryString = "match $x isa baseEntity; get;";
                String queryString2 = "match $x isa subEntity; get;";
                List<ConceptMap> answers = tx.execute(Graql.parse(queryString).asGet());
                List<ConceptMap> answers2 = tx.execute(Graql.parse(queryString2).asGet());
                assertEquals(1, answers.size());
                assertTrue(answers.containsAll(answers2));
                assertTrue(answers2.containsAll(answers));
            }
        }
    }

}
