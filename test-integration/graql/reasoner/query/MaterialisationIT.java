/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MaterialisationIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl materialisationTestSession;

    @BeforeClass
    public static void loadContext() {
        materialisationTestSession = server.sessionWithNewKeyspace();
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath,"materialisationTest.gql", materialisationTestSession);
    }

    @AfterClass
    public static void closeSession() {
        materialisationTestSession.close();
    }

    @Test
    public void whenMaterialisingRelations_newRelationsAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Iterator<Entity> entityIterator = tx.getMetaEntityType().instances().iterator();
            Entity entity = entityIterator.next();
            Entity anotherEntity = entityIterator.next();

            Statement relation = Graql.var("rel")
                    .rel("someRole", Graql.var("x"))
                    .rel("anotherRole", Graql.var("y"))
                    .isa("someRelation");
            Conjunction<Statement> pattern = Graql.and(Sets.newHashSet(
                    relation,
                    Graql.var("x").id(entity.id().getValue()),
                    Graql.var("y").id(anotherEntity.id().getValue())));
            materialiseWithoutDuplicates(pattern, tx);
        }
    }

    @Test
    public void whenMaterialisingAttributes_newAttributesAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Entity entity = tx.getMetaEntityType().instances().iterator().next();

            Statement attribute = Graql.var("x").has("resource-string", "materialised").id(entity.id().getValue());
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(attribute));
            materialiseWithoutDuplicates(pattern, tx);
        }
    }

    @Test
    public void whenMaterialisingEntities_newEntitiesAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Statement entity = Graql.var("x").isa("newEntity");
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(entity));
            ReasonerAtomicQuery entityQuery = ReasonerQueries.atomic(pattern, tx);

            Set<ConceptMap> materialised = entityQuery.materialise(new ConceptMap()).collect(toSet());
            assertTrue(tx.execute(entityQuery.getQuery()).containsAll(materialised));
        }
    }

    @Test
    public void whenMaterialisingRelationsThatAlreadyExist_noNewRelationsAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Iterator<Entity> entityIterator = tx.getMetaEntityType().instances().iterator();
            Entity entity = entityIterator.next();
            Entity anotherEntity = entityIterator.next();

            Statement relation = Graql.var("rel")
                    .rel("someRole", Graql.var("x"))
                    .rel("anotherRole", Graql.var("y"))
                    .isa("someRelation");
            Conjunction<Statement> pattern = Graql.and(Sets.newHashSet(
                    relation,
                    Graql.var("x").id(entity.id().getValue()),
                    Graql.var("y").id(anotherEntity.id().getValue())));
            materialiseWithoutDuplicates(pattern, tx);
        }
    }

    @Test
    public void whenMaterialisingAttributesThatAlreadyExist_noNewAttributesAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Entity entity = tx.getMetaEntityType().instances().iterator().next();

            Statement attribute = Graql.var("x").has("resource-string", "materialised").id(entity.id().getValue());
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(attribute));
            materialiseWithoutDuplicates(pattern, tx);
        }
    }

    @Test
    public void whenMaterialisingImplicitRelations_appropriateAttributeIsCorrectlyCreatedAndAttached() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Iterator<Entity> entityIterator = tx.getMetaEntityType().instances().iterator();
            Entity entity = entityIterator.next();

            Statement attribute = Graql.var("x").has("resource-string", "materialised").id(entity.id().getValue());
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(attribute));
            ReasonerAtomicQuery attributeQuery = ReasonerQueries.atomic(pattern, tx);
            ReasonerAtomicQuery implicitRelationQuery = ReasonerQueries.atomic(attributeQuery.getAtom().toRelationAtom());

            List<ConceptMap> materialised = materialiseWithoutDuplicates(implicitRelationQuery.getPattern(), tx);
            assertTrue(tx.execute(attributeQuery.getQuery()).containsAll(materialised));
        }
    }

    @Test
    public void whenMaterialisingEntity_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            ReasonerAtomicQuery entityQuery = ReasonerQueries.atomic(conjunction("$x isa newEntity;"), tx);
            assertTrue(entityQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("x").asEntity().isInferred());
        }
    }

    @Test
    public void whenMaterialisingAttributes_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Concept firstEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa someEntity; get;").asGet(), false)).get("x");
            Concept secondEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa anotherEntity; get;").asGet(), false)).get("x");
            Concept resource = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa resource-string; get;").asGet(), false)).get("x");

            Conjunction<Statement> resourcePattern = conjunction("{ $x has resource-string $r;$r == 'inferred';$x id " + firstEntity.id().getValue() + "; };");
            Conjunction<Statement> reuseResourcePattern = conjunction(
                    "{" +
                            " $x has resource-string $r;" +
                            " $x id " + secondEntity.id().getValue() + ";" +
                            " $r id " + resource.id().getValue() + ";" +
                            " };");

            List<ConceptMap> resourceAnswers = materialiseWithoutDuplicates(resourcePattern, tx);
            assertTrue(Iterables.getOnlyElement(resourceAnswers).get("r").asAttribute().isInferred());

            materialiseWithoutDuplicates(reuseResourcePattern, tx);
            assertTrue(Iterables.getOnlyElement(
                    tx.execute(Graql.parse("match" +
                            "$x has resource-string $r via $rel;" +
                            "$x id " + secondEntity.id().getValue() + ";" +
                            "$r id " + resource.id().getValue() + ";" +
                            "get;").asGet(), false)).get("rel").asRelation().isInferred());
            assertFalse(Iterables.getOnlyElement(
                    tx.execute(Graql.parse("match" +
                            "$x has resource-string $r via $rel;" +
                            "$x id " + firstEntity.id().getValue() + ";" +
                            "$r id " + resource.id().getValue() + ";" +
                            "get;").asGet(), false)).get("rel").asRelation().isInferred());
        }
    }

    @Test
    public void whenMaterialisingAttributesWithCompatibleValues_noDuplicatesAreCreated() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Entity entity = tx.getMetaEntityType().instances().iterator().next();

            materialiseWithoutDuplicates(Graql.var("x").has("resource-string", "materialised").id(entity.id().getValue()), tx);
            materialiseWithoutDuplicates(Graql.var("x").has("resource-boolean", true).id(entity.id().getValue()), tx);

            materialiseWithoutDuplicates(Graql.var("x").has("resource-double", 10L).id(entity.id().getValue()), tx);
            materialiseWithoutDuplicates(Graql.var("x").has("resource-double", 10).id(entity.id().getValue()), tx);
            materialiseWithoutDuplicates(Graql.var("x").has("resource-double", 10.0).id(entity.id().getValue()), tx);

            materialiseWithoutDuplicates(Graql.var("x").has("resource-long", 10.0).id(entity.id().getValue()), tx);
            materialiseWithoutDuplicates(Graql.var("x").has("resource-long", 10).id(entity.id().getValue()), tx);
            materialiseWithoutDuplicates(Graql.var("x").has("resource-long", 10L).id(entity.id().getValue()), tx);

            materialiseWithoutDuplicates(Graql.var("x").has("resource-date", LocalDateTime.now()).id(entity.id().getValue()), tx);
            materialiseWithoutDuplicates(Graql.parsePattern("$x id " + entity.id().getValue() + ", has resource-date " + LocalDate.now() + ";").statements().iterator().next(), tx);
        }
    }


    @Test
    public void whenMaterialisingRelations_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Concept firstEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa someEntity; get;").asGet(), false)).get("x");
            Concept secondEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa anotherEntity; get;").asGet(), false)).get("x");

            Conjunction<Statement> relationConj = conjunction(
                    "{" +
                            " $r (someRole: $x, anotherRole: $y);" +
                            " $x id " + firstEntity.id().getValue() + ";" +
                            " $y id " + secondEntity.id().getValue() + ";" +
                            " };"
            );
            Relation materialisedRelation = Iterables.getOnlyElement(materialiseWithoutDuplicates(relationConj, tx)).get("r").asRelation();
            assertTrue(materialisedRelation.isInferred());
        }
    }

    private List<ConceptMap> materialiseWithoutDuplicates(Pattern pattern, TransactionOLTP tx){
        return materialiseWithoutDuplicates(Graql.and(pattern.statements()), tx);
    }

    private List<ConceptMap> materialiseWithoutDuplicates(Statement statement, TransactionOLTP tx){
        return materialiseWithoutDuplicates(Graql.and(Collections.singleton(statement)), tx);
    }

    private List<ConceptMap> materialiseWithoutDuplicates(Conjunction<Statement> statement, TransactionOLTP tx){
        ReasonerAtomicQuery query = ReasonerQueries.atomic(statement, tx);

        List<ConceptMap> materialised = query.materialise(new ConceptMap()).collect(Collectors.toList());
        List<ConceptMap> answers = tx.execute(query.getQuery());
        assertTrue(answers.containsAll(materialised));
        List<ConceptMap> reMaterialised = query.materialise(new ConceptMap()).collect(Collectors.toList());
        assertCollectionsNonTriviallyEqual(materialised, reMaterialised);
        assertCollectionsNonTriviallyEqual(answers, tx.execute(query.getQuery()));
        return materialised;
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
