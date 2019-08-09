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
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
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
            ReasonerAtomicQuery relationQuery = ReasonerQueries.atomic(pattern, tx);

            Set<ConceptMap> materialised = relationQuery.materialise(new ConceptMap()).collect(toSet());
            assertTrue(tx.execute(relationQuery.getQuery()).containsAll(materialised));
        }
    }

    @Test
    public void whenMaterialisingAttributes_newAttributesAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Entity entity = tx.getMetaEntityType().instances().iterator().next();

            Statement attribute = Graql.var("x").has("resource", "materialised").id(entity.id().getValue());
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(attribute));
            ReasonerAtomicQuery attributeQuery = ReasonerQueries.atomic(pattern, tx);

            Set<ConceptMap> materialised = attributeQuery.materialise(new ConceptMap()).collect(toSet());
            assertTrue(tx.execute(attributeQuery.getQuery()).containsAll(materialised));
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
            ReasonerAtomicQuery relationQuery = ReasonerQueries.atomic(pattern, tx);

            List<ConceptMap> inserted = tx.execute(Graql.insert(pattern.statements()));
            List<ConceptMap> postInsertAnswers = tx.execute(relationQuery.getQuery());
            Set<ConceptMap> materialised = relationQuery.materialise(new ConceptMap()).collect(toSet());
            assertCollectionsNonTriviallyEqual(postInsertAnswers, tx.execute(relationQuery.getQuery()));
        }
    }

    @Test
    public void whenMaterialisingAttributesThatAlreadyExist_noNewAttributesAreInsertedInTheGraph() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Entity entity = tx.getMetaEntityType().instances().iterator().next();

            Statement attribute = Graql.var("x").has("resource", "materialised").id(entity.id().getValue());
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(attribute));
            ReasonerAtomicQuery attributeQuery = ReasonerQueries.atomic(pattern, tx);

            List<ConceptMap> inserted = tx.execute(Graql.insert(pattern.statements()));
            List<ConceptMap> postInsertAnswers = tx.execute(attributeQuery.getQuery());
            Set<ConceptMap> materialised = attributeQuery.materialise(new ConceptMap()).collect(toSet());
            assertCollectionsNonTriviallyEqual(postInsertAnswers, tx.execute(attributeQuery.getQuery()));
        }
    }

    @Test
    public void whenMaterialisingImplicitRelations_appropriateAttributeIsCorrectlyCreatedAndAttached() {
        try(TransactionOLTP tx = materialisationTestSession.transaction().write()) {
            Iterator<Entity> entityIterator = tx.getMetaEntityType().instances().iterator();
            Entity entity = entityIterator.next();

            Statement attribute = Graql.var("x").has("resource", "materialised").id(entity.id().getValue());
            Conjunction<Statement> pattern = Graql.and(Collections.singleton(attribute));
            ReasonerAtomicQuery attributeQuery = ReasonerQueries.atomic(pattern, tx);
            ReasonerAtomicQuery implicitRelationQuery = ReasonerQueries.atomic(attributeQuery.getAtom().toRelationAtom());

            Set<ConceptMap> materialised = implicitRelationQuery.materialise(new ConceptMap()).collect(toSet());
            assertTrue(tx.execute(attributeQuery.getQuery()).containsAll(materialised));
        }
    }

    @Test
    public void whenMaterialisingEntity_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        TransactionOLTP tx = materialisationTestSession.transaction().write();
        ReasonerAtomicQuery entityQuery = ReasonerQueries.atomic(conjunction("$x isa newEntity;"), tx);
        assertTrue(entityQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("x").asEntity().isInferred());
        tx.close();
    }

    @Test
    public void whenMaterialisingResources_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        TransactionOLTP tx = materialisationTestSession.transaction().write();
        Concept firstEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa someEntity; get;").asGet(), false)).get("x");
        Concept secondEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa anotherEntity; get;").asGet(), false)).get("x");
        Concept resource = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa resource; get;").asGet(), false)).get("x");

        ReasonerAtomicQuery resourceQuery = ReasonerQueries.atomic(conjunction("{ $x has resource $r;$r == 'inferred';$x id " + firstEntity.id().getValue() + "; };"), tx);
        String reuseResourcePatternString =
                "{" +
                        " $x has resource $r;" +
                        " $x id " + secondEntity.id().getValue() + ";" +
                        " $r id " + resource.id().getValue() + ";" +
                        " };";

        ReasonerAtomicQuery reuseResourceQuery = ReasonerQueries.atomic(conjunction(reuseResourcePatternString), tx);

        assertTrue(resourceQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("r").asAttribute().isInferred());

        List<ConceptMap> answers = reuseResourceQuery.materialise(new ConceptMap()).collect(Collectors.toList());
        assertTrue(Iterables.getOnlyElement(
                tx.execute(Graql.parse("match" +
                        "$x has resource $r via $rel;" +
                        "$x id " + secondEntity.id().getValue() + ";" +
                        "$r id " + resource.id().getValue() + ";" +
                        "get;").asGet(), false)).get("rel").asRelation().isInferred());
        assertFalse(Iterables.getOnlyElement(
                tx.execute(Graql.parse("match" +
                        "$x has resource $r via $rel;" +
                        "$x id " + firstEntity.id().getValue() + ";" +
                        "$r id " + resource.id().getValue() + ";" +
                        "get;").asGet(), false)).get("rel").asRelation().isInferred());
        tx.close();
    }

    @Test
    public void whenMaterialisingRelations_MaterialisedInformationIsCorrectlyFlaggedAsInferred() {
        TransactionOLTP tx = materialisationTestSession.transaction().write();
        Concept firstEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa someEntity; get;").asGet(), false)).get("x");
        Concept secondEntity = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x isa anotherEntity; get;").asGet(), false)).get("x");

        ReasonerAtomicQuery relationQuery = ReasonerQueries.atomic(conjunction(
                "{" +
                        " $r (someRole: $x, anotherRole: $y);" +
                        " $x id " + firstEntity.id().getValue() + ";" +
                        " $y id " + secondEntity.id().getValue() + ";" +
                        " };"
                ),
                tx
        );

        assertTrue(relationQuery.materialise(new ConceptMap()).findFirst().orElse(null).get("r").asRelation().isInferred());
        tx.close();
    }

    private Conjunction<Statement> conjunction(String patternString) {
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
