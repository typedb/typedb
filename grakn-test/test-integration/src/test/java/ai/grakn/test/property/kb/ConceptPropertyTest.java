/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.property.kb;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
import ai.grakn.generator.FromTxGenerator.FromTx;
import ai.grakn.generator.GraknTxs.Open;
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

import static ai.grakn.generator.GraknTxs.allConceptsFrom;
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
import static org.junit.Assert.assertFalse;
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

        Object[] params = mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(isA(RuntimeException.class));

        method.invoke(concept, params);
    }

    @Ignore // TODO: Either fix this, remove test or add exceptions to this rule
    @Property
    public void whenCallingAnyMethodsOnAConceptFromAClosedGraph_Throw(
            @Open GraknTx graph, @FromTx Concept concept, @MethodOf(Concept.class) Method method) throws Throwable {
        graph.close();

        Object[] params = mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(isA(GraknTxOperationException.class));
        exception.expectCause(hasProperty("message", is(ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("closed", graph.keyspace()))));

        method.invoke(concept, params);
    }

    @Property
    public void whenCallingToStringOnADeletedConcept_TheStringContainsTheId(
            @Open GraknTx graph, @FromTx @NonMeta Concept concept) {
        assumeDeletable(graph, concept);
        concept.delete();
        assertThat(concept.toString(), containsString(concept.id().getValue()));
    }

    @Property
    public void whenCallingGetId_TheResultIsUnique(Concept concept1, @FromTx Concept concept2) {
        assumeThat(concept1, not(is(concept2)));
        assertNotEquals(concept1.id(), concept2.id());
    }

    @Property
    public void whenCallingGetId_TheResultCanBeUsedToRetrieveTheSameConcept(
            @Open GraknTx graph, @FromTx Concept concept) {
        ConceptId id = concept.id();
        assertEquals(concept, graph.getConcept(id));
    }

    @Property
    public void whenConceptIsNotARule_TheConceptCannotBeConvertedToARule(Concept concept) {
        assumeFalse(concept.isRule());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRule();
    }

    @Property
    public void whenCallingDelete_TheConceptIsNoLongerInTheGraph(
            @Open GraknTx graph, @NonMeta @FromTx Concept concept) {
        assumeDeletable(graph, concept);

        assertThat(allConceptsFrom(graph), hasItem(concept));
        concept.delete();
        assertThat(allConceptsFrom(graph), not(hasItem(concept)));
    }

    @Property
    public void whenAConceptIsNotDeleted_CallingIsDeletedReturnsFalse(Concept concept) {
        assertFalse(concept.isDeleted());
    }

    @Property
    public void whenConceptIsASubClass_TheConceptCanBeConvertedToThatSubClass(Concept concept) {
        // These are all in one test only because they are trivial

        if (concept.isSchemaConcept()) assertEquals(concept, concept.asSchemaConcept());

        if (concept.isType()) assertEquals(concept, concept.asType());

        if (concept.isEntityType()) assertEquals(concept, concept.asEntityType());

        if (concept.isRelationshipType()) assertEquals(concept, concept.asRelationshipType());

        if (concept.isRole()) assertEquals(concept, concept.asRole());

        if (concept.isAttributeType()) assertEquals(concept, concept.asAttributeType());

        if (concept.isRule()) assertEquals(concept, concept.asRule());

        if (concept.isThing()) assertEquals(concept, concept.asThing());

        if (concept.isEntity()) assertEquals(concept, concept.asEntity());

        if (concept.isRelationship()) assertEquals(concept, concept.asRelationship());

        if (concept.isAttribute()) assertEquals(concept, concept.asAttribute());

    }

    @Property
    public void whenConceptIsNotAType_TheConceptCannotBeConvertedToAType(Concept concept) {
        assumeFalse(concept.isType());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asType();
    }

    @Property
    public void whenConceptIsNotAnSchemaConcept_TheConceptCannotBeConvertedToAnSchemaConcept(Concept concept) {
        assumeFalse(concept.isSchemaConcept());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asSchemaConcept();
    }

    @Property
    public void whenConceptIsNotAnEntityType_TheConceptCannotBeConvertedToAnEntityType(Concept concept) {
        assumeFalse(concept.isEntityType());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asEntityType();
    }

    @Property
    public void whenConceptIsNotARelationType_TheConceptCannotBeConvertedToARelationType(Concept concept) {
        assumeFalse(concept.isRelationshipType());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRelationshipType();
    }

    @Property
    public void whenConceptIsNotARole_TheConceptCannotBeConvertedToARole(Concept concept) {
        assumeFalse(concept.isRole());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRole();
    }

    @Property
    public void whenConceptIsNotAResourceType_TheConceptCannotBeConvertedToAResourceType(Concept concept) {
        assumeFalse(concept.isAttributeType());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asAttributeType();
    }

    @Property
    public void whenConceptIsNotARuleType_TheConceptCannotBeConvertedToARuleType(Concept concept) {
        assumeFalse(concept.isRule());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRule();
    }

    @Property
    public void whenConceptIsNotAnInstance_TheConceptCannotBeConvertedToAnInstance(Concept concept) {
        assumeFalse(concept.isThing());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asThing();
    }

    @Property
    public void whenConceptIsNotAnEntity_TheConceptCannotBeConvertedToAnEntity(Concept concept) {
        assumeFalse(concept.isEntity());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asEntity();
    }

    @Property
    public void whenConceptIsNotARelation_TheConceptCannotBeConvertedToARelation(Concept concept) {
        assumeFalse(concept.isRelationship());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asRelationship();
    }

    @Property
    public void whenConceptIsNotAResource_TheConceptCannotBeConvertedToAResource(Concept concept) {
        assumeFalse(concept.isAttribute());
        exception.expect(GraknTxOperationException.class);
        //noinspection ResultOfMethodCallIgnored
        concept.asAttribute();
    }

    private static void assumeDeletable(GraknTx graph, Concept concept) {
        // Confirm this concept is allowed to be deleted
        // TODO: A better way to handle these assumptions?
        if (concept.isSchemaConcept()) {
            SchemaConcept schemaConcept = concept.asSchemaConcept();
            assumeThat(schemaConcept.subs().collect(toSet()), contains(schemaConcept));
            if(schemaConcept.isType()) {
                Type type = schemaConcept.asType();
                assumeThat(type.instances().collect(toSet()), empty());
                assumeThat(type.whenRules().collect(toSet()), empty());
                assumeThat(type.thenRules().collect(toSet()), empty());
            }

            if (schemaConcept.isRole()) {
                Role role = schemaConcept.asRole();
                assumeThat(role.players().collect(toSet()), empty());
                assumeThat(role.relationships().collect(toSet()), empty());
                Stream<Relationship> allRelations = graph.admin().getMetaRelationType().instances();
                Set<Role> allRolesPlayed = allRelations.flatMap(relation -> relation.rolePlayersMap().keySet().stream()).collect(toSet());
                assumeThat(allRolesPlayed, not(hasItem(role)));
            } else if (schemaConcept.isRelationshipType()) {
                assumeThat(schemaConcept.asRelationshipType().relates().collect(toSet()), empty());
            }
        }
    }

}