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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
import ai.grakn.generator.AbstractTypeGenerator.NonAbstract;
import ai.grakn.generator.FromTxGenerator.FromTx;
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.generator.Methods.MethodOf;
import ai.grakn.generator.ResourceValues;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static ai.grakn.generator.GraknTxs.allConceptsFrom;
import static ai.grakn.generator.GraknTxs.allSchemaElementsFrom;
import static ai.grakn.generator.Methods.mockParamsOf;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;


@RunWith(JUnitQuickcheck.class)
public class GraknTxPropertyTest {

    @org.junit.Rule
    public ExpectedException exception = ExpectedException.none();

    @Ignore("Some times creates null labels resulting in NPE. This is unavoidable. We need to change the definition of this test")
    @Property
    public void whenCallingMostMethodOnAClosedGraph_Throw(
            @Open(false) GraknTx graph, @MethodOf(GraknTx.class) Method method) throws Throwable {

        // TODO: Should `admin`, `close`, `implicitConceptsVisible`, `getKeyspace` and `graql` be here?
        assumeThat(method.getName(), not(isOneOf(
                "isClosed", "admin", "close", "commit", "abort", "isReadOnly", "implicitConceptsVisible",
                "getKeyspace", "graql", "id"
        )));
        Object[] params = mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(isA(GraknTxOperationException.class));
        exception.expectCause(hasProperty("message", is(ErrorMessage.TX_CLOSED_ON_ACTION.getMessage("closed", graph.keyspace()))));

        method.invoke(graph, params);
    }

    @Property
    public void whenCallingGetConceptWithAnExistingConceptId_ItReturnsThatConcept(
            @Open GraknTx graph, @FromTx Concept concept) {
        ConceptId id = concept.id();
        assertEquals(concept, graph.getConcept(id));
    }

    @Property
    public void whenCallingGetConceptWithANonExistingConceptId_ItReturnsNull(@Open GraknTx graph, ConceptId id) {
        Set<ConceptId> allIds = allConceptsFrom(graph).stream().map(Concept::id).collect(toSet());
        assumeThat(allIds, not(hasItem(id)));

        assertNull(graph.getConcept(id));
    }

    @Property
    public void whenCallingGetConceptWithAnIncorrectGeneric_ItThrows(
            @Open GraknTx graph, @FromTx Concept concept) {
        assumeFalse(concept.isRole());
        ConceptId id = concept.id();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        Role role = graph.getConcept(id);
    }

    @Property
    public void whenCallingGetSchemaConceptWithAnExistingLabel_ItReturnsThatConcept(
            @Open GraknTx graph, @FromTx SchemaConcept concept) {
        Label label = concept.label();
        assertEquals(concept, graph.getSchemaConcept(label));
    }

    @Property
    public void whenCallingGetSchemaConceptWithANonExistingTypeLabel_ItReturnsNull(
            @Open GraknTx graph, Label label) {
        Set<Label> allTypes = allSchemaElementsFrom(graph).stream().map(SchemaConcept::label).collect(toSet());
        assumeThat(allTypes, not(hasItem(label)));

        assertNull(graph.getSchemaConcept(label));
    }

    @Property
    public void whenCallingGetSchemaConceptWithAnIncorrectGeneric_ItThrows(
            @Open GraknTx graph, @FromTx Type type) {
        assumeFalse(type.isRole());
        Label label = type.label();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        Role role = graph.getSchemaConcept(label);
    }

    @Property
    public void whenCallingGetResourcesByValueAfterAddingAResource_TheResultIncludesTheResource(
            @Open GraknTx graph,
            @FromTx @NonMeta @NonAbstract AttributeType attributeType, @From(ResourceValues.class) Object value) {
        assumeThat(value.getClass().getName(), is(attributeType.dataType().getName()));

        Collection<Attribute<Object>> expectedAttributes = graph.getAttributesByValue(value);
        Attribute attribute = attributeType.create(value);
        Collection<Attribute<Object>> resourcesAfter = graph.getAttributesByValue(value);

        expectedAttributes.add(attribute);

        assertEquals(expectedAttributes, resourcesAfter);
    }

    @Property
    public void whenCallingGetResourcesByValueAfterDeletingAResource_TheResultDoesNotIncludesTheResource(
            @Open GraknTx graph, @FromTx Attribute<Object> attribute) {
        Object resourceValue = attribute.value();

        Collection<Attribute<Object>> expectedAttributes = graph.getAttributesByValue(resourceValue);
        attribute.delete();
        Collection<Attribute<Object>> resourcesAfter = graph.getAttributesByValue(resourceValue);

        expectedAttributes.remove(attribute);

        assertEquals(expectedAttributes, resourcesAfter);
    }

    @Property
    public void whenCallingGetResourcesByValue_TheResultIsAllResourcesWithTheGivenValue(
            @Open GraknTx graph, @From(ResourceValues.class) Object resourceValue) {
        Stream<Attribute<?>> allResources = graph.admin().getMetaAttributeType().instances();

        Set<Attribute<?>> allResourcesOfValue =
                allResources.filter(resource -> resourceValue.equals(resource.value())).collect(toSet());

        assertEquals(allResourcesOfValue, graph.getAttributesByValue(resourceValue));
    }

    @Property
    public void whenCallingGetResourcesByValueWithAnUnsupportedDataType_Throw(@Open GraknTx graph, List value) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.unsupportedDataType(value).getMessage());
        //noinspection ResultOfMethodCallIgnored
        graph.getAttributesByValue(value);
    }

    @Property
    public void whenCallingGetEntityType_TheResultIsTheSameAsGetSchemaConcept(
            @Open GraknTx graph, @FromTx EntityType type) {
        Label label = type.label();
        assertSameResult(() -> graph.getSchemaConcept(label), () -> graph.getEntityType(label.getValue()));
    }

    @Property
    public void whenCallingGetRelationType_TheResultIsTheSameAsGetSchemaConcept(
            @Open GraknTx graph, @FromTx RelationshipType type) {
        Label label = type.label();
        assertSameResult(() -> graph.getSchemaConcept(label), () -> graph.getRelationshipType(label.getValue()));
    }

    @Property
    public void whenCallingGetResourceType_TheResultIsTheSameAsGetSchemaConcept(
            @Open GraknTx graph, @FromTx AttributeType type) {
        Label label = type.label();
        assertSameResult(() -> graph.getSchemaConcept(label), () -> graph.getAttributeType(label.getValue()));
    }

    @Property
    public void whenCallingGetRole_TheResultIsTheSameAsGetSchemaConcept(
            @Open GraknTx graph, @FromTx Role role) {
        Label label = role.label();
        assertSameResult(() -> graph.getSchemaConcept(label), () -> graph.getRole(label.getValue()));
    }

    @Property
    public void whenCallingGetRule_TheResultIsTheSameAsGetSchemaConcept(
            @Open GraknTx graph, @FromTx Rule type) {
        Label label = type.label();
        assertSameResult(() -> graph.getSchemaConcept(label), () -> graph.getRule(label.getValue()));
    }

    @Property
    public void whenCallingAdmin_TheResultIsTheSameGraph(GraknTx graph) {
        assertEquals(graph, graph.admin());
    }

    //TODO: move the following 3 tests to Grakn.Keyspace tests!
//    @Property
//    public void whenCallingDelete_TheTransactionCloses(@Open GraknTx graph) {
//        graph.admin().delete();
//        assertTrue(graph.isClosed());
//    }
//
//    @Property
//    public void whenCallingClear_OnlyMetaConceptsArePresent(@Open GraknTx graph) {
//        graph.admin().delete();
//        graph = EmbeddedGraknSession.inMemory( graph.keyspace()).transaction(GraknTxType.WRITE);
//        List<Concept> concepts = allConceptsFrom(graph);
//        concepts.forEach(concept -> {
//            assertTrue(concept.isSchemaConcept());
//            assertTrue(isMetaLabel(concept.asSchemaConcept().label()));
//        });
//        graph.close();
//    }
//
//    @Property
//    public void whenCallingDeleteAndReOpening_AllMetaConceptsArePresent(@Open GraknTx graph, @From(MetaLabels.class) Label label) {
//        graph.admin().delete();
//        graph = EmbeddedGraknSession.inMemory( graph.keyspace()).transaction(GraknTxType.WRITE);
//        assertNotNull(graph.getSchemaConcept(label));
//        graph.close();
//    }

    @Property
    public void whenCallingIsClosedOnAClosedGraph_ReturnTrue(@Open(false) GraknTx graph) {
        assertTrue(graph.isClosed());
    }

    @Property
    public void whenCallingIsClosedOnAnOpenGraph_ReturnFalse(@Open GraknTx graph) {
        assertFalse(graph.isClosed());
    }

    @Property
    public void whenCallingClose_TheGraphIsClosed(GraknTx graph) throws InvalidKBException {
        graph.close();
        assertTrue(graph.isClosed());
    }


    // TODO: Everything below this point should be moved to more appropriate test classes

    @Property
    public void whenSetRegexOnMetaResourceType_Throw(@Open GraknTx graph, String regex) {
        AttributeType resource = graph.admin().getMetaAttributeType();

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.cannotSetRegex(resource).getMessage());

        resource.regex(regex);
    }

    @Property
    public void whenCreateInstanceOfMetaResourceType_Throw(
            @Open GraknTx graph, @From(ResourceValues.class) Object value) {
        AttributeType resource = graph.admin().getMetaAttributeType();

        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(resource.label()).getMessage());

        resource.create(value);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingSuperTypeOnMetaResourceType_Throw(@Open GraknTx graph) {
        AttributeType resource = graph.admin().getMetaAttributeType();

        // TODO: Test for a better error message
        exception.expect(GraknTxOperationException.class);

        //noinspection ResultOfMethodCallIgnored
        resource.sup();
    }

    @Property
    public void whenCallingHasWithMetaResourceType_ThrowMetaTypeImmutableException(
            @Open GraknTx graph, @FromTx Type type) {
        AttributeType resource = graph.admin().getMetaAttributeType();

        exception.expect(GraknTxOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.label())) {
            exception.expectMessage(GraknTxOperationException.metaTypeImmutable(type.label()).getMessage());
        } else {
            exception.expectMessage(GraknTxOperationException.metaTypeImmutable(resource.label()).getMessage());
        }
        type.has(resource);
    }

    private <T> void assertSameResult(Supplier<T> expectedMethod, Supplier<T> actualMethod) {
        T expectedResult = null;
        Exception expectedException = null;

        try {
            expectedResult = expectedMethod.get();
        } catch (Exception e) {
            expectedException = e;
        }

        T actualResult = null;
        Exception actualException = null;

        try {
            actualResult = actualMethod.get();
        } catch (Exception e) {
            actualException = e;
        }

        assertEquals(expectedException, actualException);
        assertEquals(expectedResult, actualResult);
    }
}