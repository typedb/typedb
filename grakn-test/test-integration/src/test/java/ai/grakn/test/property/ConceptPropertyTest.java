/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.test.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import ai.grakn.generator.Methods.MethodOf;
import ai.grakn.util.ErrorMessage;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.generator.GraknGraphs.allConceptsFrom;
import static ai.grakn.generator.Methods.mockParamsOf;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class ConceptPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Ignore // TODO: Either fix this, remove test or add exceptions to this rule
    @Property
    public void whenCallingAnyMethodOnADeletedConcept_Throw(
            @NonMeta Concept concept, @MethodOf(Concept.class) Method method) throws Throwable {
        concept.delete();

        Object[] params = Methods.mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(Matchers.isA(RuntimeException.class));

        method.invoke(concept, params);
    }

    @Ignore // TODO: Either fix this, remove test or add exceptions to this rule
    @Property
    public void whenCallingAnyMethodsOnAConceptFromAClosedGraph_Throw(
            @Open GraknGraph graph, @FromGraph Concept concept, @MethodOf(Concept.class) Method method) throws Throwable {
        graph.close();

        Object[] params = Methods.mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(Matchers.isA(GraphOperationException.class));
        exception.expectCause(Matchers.hasProperty("message", Matchers.is(ErrorMessage.GRAPH_CLOSED_ON_ACTION.getMessage("closed", graph.getKeyspace()))));

        method.invoke(concept, params);
    }

    @Property
    public void whenCallingToStringOnADeletedConcept_TheStringContainsTheId(
            @Open GraknGraph graph, @FromGraph @NonMeta Concept concept) {
        assumeDeletable(graph, concept);
        concept.delete();
        MatcherAssert.assertThat(concept.toString(), Matchers.containsString(concept.getId().getValue()));
    }

    @Property
    public void whenCallingGetId_TheResultIsUnique(Concept concept1, @FromGraph Concept concept2) {
        Assume.assumeThat(concept1, Matchers.not(Matchers.is(concept2)));
        Assert.assertNotEquals(concept1.getId(), concept2.getId());
    }

    @Property
    public void whenCallingGetId_TheResultCanBeUsedToRetrieveTheSameConcept(
            @Open GraknGraph graph, @FromGraph Concept concept) {
        ConceptId id = concept.getId();
        Assert.assertEquals(concept, graph.getConcept(id));
    }

    @Property
    public void whenCallingDelete_TheConceptIsNoLongerInTheGraph(
            @Open GraknGraph graph, @NonMeta @FromGraph Concept concept) {
        assumeDeletable(graph, concept);

        MatcherAssert.assertThat(GraknGraphs.allConceptsFrom(graph), Matchers.hasItem(concept));
        concept.delete();
        MatcherAssert.assertThat(GraknGraphs.allConceptsFrom(graph), Matchers.not(Matchers.hasItem(concept)));
    }

    @Property
    public void whenConceptIsASubClass_TheConceptCanBeConvertedToThatSubClass(Concept concept) {
        // These are all in one test only because they are trivial

        if (concept.isOntologyConcept()) Assert.assertEquals(concept, concept.asOntologyConcept());

        if (concept.isType()) Assert.assertEquals(concept, concept.asType());

        if (concept.isEntityType()) Assert.assertEquals(concept, concept.asEntityType());

        if (concept.isRelationType()) Assert.assertEquals(concept, concept.asRelationType());

        if (concept.isRole()) Assert.assertEquals(concept, concept.asRole());

        if (concept.isResourceType()) Assert.assertEquals(concept, concept.asResourceType());

        if (concept.isRuleType()) Assert.assertEquals(concept, concept.asRuleType());

        if (concept.isThing()) Assert.assertEquals(concept, concept.asThing());

        if (concept.isEntity()) Assert.assertEquals(concept, concept.asEntity());

        if (concept.isRelation()) Assert.assertEquals(concept, concept.asRelation());

        if (concept.isResource()) Assert.assertEquals(concept, concept.asResource());

        if (concept.isRule()) Assert.assertEquals(concept, concept.asRule());
    }

    @Property
    public void whenConceptIsNotAType_TheConceptCannotBeConvertedToAType(Concept concept) {
        Assume.assumeFalse(concept.isType());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asType();
    }

    @Property
    public void whenConceptIsNotAnOntologyConcept_TheConceptCannotBeConvertedToAnOntologyConcept(Concept concept) {
        Assume.assumeFalse(concept.isOntologyConcept());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asOntologyConcept();
    }

    @Property
    public void whenConceptIsNotAnEntityType_TheConceptCannotBeConvertedToAnEntityType(Concept concept) {
        Assume.assumeFalse(concept.isEntityType());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asEntityType();
    }

    @Property
    public void whenConceptIsNotARelationType_TheConceptCannotBeConvertedToARelationType(Concept concept) {
        Assume.assumeFalse(concept.isRelationType());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRelationType();
    }

    @Property
    public void whenConceptIsNotARole_TheConceptCannotBeConvertedToARole(Concept concept) {
        Assume.assumeFalse(concept.isRole());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRole();
    }

    @Property
    public void whenConceptIsNotAResourceType_TheConceptCannotBeConvertedToAResourceType(Concept concept) {
        Assume.assumeFalse(concept.isResourceType());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asResourceType();
    }

    @Property
    public void whenConceptIsNotARuleType_TheConceptCannotBeConvertedToARuleType(Concept concept) {
        Assume.assumeFalse(concept.isRuleType());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRuleType();
    }

    @Property
    public void whenConceptIsNotAnInstance_TheConceptCannotBeConvertedToAnInstance(Concept concept) {
        Assume.assumeFalse(concept.isThing());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asThing();
    }

    @Property
    public void whenConceptIsNotAnEntity_TheConceptCannotBeConvertedToAnEntity(Concept concept) {
        Assume.assumeFalse(concept.isEntity());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asEntity();
    }

    @Property
    public void whenConceptIsNotARelation_TheConceptCannotBeConvertedToARelation(Concept concept) {
        Assume.assumeFalse(concept.isRelation());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRelation();
    }

    @Property
    public void whenConceptIsNotAResource_TheConceptCannotBeConvertedToAResource(Concept concept) {
        Assume.assumeFalse(concept.isResource());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asResource();
    }

    @Property
    public void whenConceptIsNotARule_TheConceptCannotBeConvertedToARule(Concept concept) {
        Assume.assumeFalse(concept.isRule());
        exception.expect(GraphOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRule();
    }

    private static void assumeDeletable(GraknGraph graph, Concept concept) {
        // Confirm this concept is allowed to be deleted
        // TODO: A better way to handle these assumptions?
        if (concept.isOntologyConcept()) {
            OntologyConcept ontologyConcept = concept.asOntologyConcept();
            Assume.assumeThat(ontologyConcept.subs().collect(toSet()), Matchers.contains(ontologyConcept));
            if(ontologyConcept.isType()) {
                Type type = ontologyConcept.asType();
                Assume.assumeThat(type.instances().collect(toSet()), Matchers.empty());
                Assume.assumeThat(type.getRulesOfHypothesis().collect(toSet()), Matchers.empty());
                Assume.assumeThat(type.getRulesOfConclusion().collect(toSet()), Matchers.empty());
            }

            if (ontologyConcept.isRole()) {
                Role role = ontologyConcept.asRole();
                Assume.assumeThat(role.playedByTypes().collect(toSet()), Matchers.empty());
                Assume.assumeThat(role.relationTypes().collect(toSet()), Matchers.empty());
                Stream<Relation> allRelations = graph.admin().getMetaRelationType().instances();
                Set<Role> allRolesPlayed = allRelations.flatMap(relation -> relation.allRolePlayers().keySet().stream()).collect(toSet());
                Assume.assumeThat(allRolesPlayed, Matchers.not(Matchers.hasItem(role)));
            } else if (ontologyConcept.isRelationType()) {
                Assume.assumeThat(ontologyConcept.asRelationType().relates().collect(toSet()), Matchers.empty());
            }
        }
    }

}
