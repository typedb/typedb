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

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import ai.grakn.generator.AbstractTypeGenerator.NonAbstract;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import ai.grakn.generator.MetaLabels;
import ai.grakn.generator.Methods.MethodOf;
import ai.grakn.generator.ResourceValues;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static ai.grakn.generator.GraknGraphs.allConceptsFrom;
import static ai.grakn.generator.GraknGraphs.allOntologyElementsFrom;
import static ai.grakn.generator.Methods.mockParamsOf;
import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class GraknGraphPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenCallingMostMethodOnAClosedGraph_Throw(
            @Open(false) GraknGraph graph, @MethodOf(GraknGraph.class) Method method) throws Throwable {

        // TODO: Should `admin`, `close`, `implicitConceptsVisible`, `getKeyspace` and `graql` be here?
        assumeThat(method.getName(), not(isOneOf(
                "isClosed", "admin", "close", "commit", "abort", "isReadOnly", "implicitConceptsVisible",
                "getKeyspace", "graql", "getId"
        )));
        Object[] params = mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(isA(GraphOperationException.class));
        exception.expectCause(hasProperty("message", is(ErrorMessage.GRAPH_CLOSED_ON_ACTION.getMessage("closed", graph.getKeyspace()))));

        method.invoke(graph, params);
    }

    @Property
    public void whenCallingGetConceptWithAnExistingConceptId_ItReturnsThatConcept(
            @Open GraknGraph graph, @FromGraph Concept concept) {
        ConceptId id = concept.getId();
        assertEquals(concept, graph.getConcept(id));
    }

    @Property
    public void whenCallingGetConceptWithANonExistingConceptId_ItReturnsNull(@Open GraknGraph graph, ConceptId id) {
        Set<ConceptId> allIds = allConceptsFrom(graph).stream().map(Concept::getId).collect(toSet());
        assumeThat(allIds, not(hasItem(id)));

        assertNull(graph.getConcept(id));
    }

    @Property
    public void whenCallingGetConceptWithAnIncorrectGeneric_ItThrows(
            @Open GraknGraph graph, @FromGraph Concept concept) {
        assumeFalse(concept.isRole());
        ConceptId id = concept.getId();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        Role role = graph.getConcept(id);
    }

    @Property
    public void whenCallingGetOntologyConceptWithAnExistingLabel_ItReturnsThatConcept(
            @Open GraknGraph graph, @FromGraph OntologyConcept concept) {
        Label label = concept.getLabel();
        assertEquals(concept, graph.getOntologyConcept(label));
    }

    @Property
    public void whenCallingGetOntologyConceptWithANonExistingTypeLabel_ItReturnsNull(
            @Open GraknGraph graph, Label label) {
        Set<Label> allTypes = allOntologyElementsFrom(graph).stream().map(OntologyConcept::getLabel).collect(toSet());
        assumeThat(allTypes, not(hasItem(label)));

        assertNull(graph.getOntologyConcept(label));
    }

    @Property
    public void whenCallingGetOntologyConceptWithAnIncorrectGeneric_ItThrows(
            @Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(type.isRole());
        Label label = type.getLabel();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        Role role = graph.getOntologyConcept(label);
    }

    @Property
    public void whenCallingGetResourcesByValueAfterAddingAResource_TheResultIncludesTheResource(
            @Open GraknGraph graph,
            @FromGraph @NonMeta @NonAbstract ResourceType resourceType, @From(ResourceValues.class) Object value) {
        assumeThat(value.getClass().getName(), is(resourceType.getDataType().getName()));

        Collection<Resource<Object>> expectedResources = graph.getResourcesByValue(value);
        Resource resource = resourceType.putResource(value);
        Collection<Resource<Object>> resourcesAfter = graph.getResourcesByValue(value);

        expectedResources.add(resource);

        assertEquals(expectedResources, resourcesAfter);
    }

    @Property
    public void whenCallingGetResourcesByValueAfterDeletingAResource_TheResultDoesNotIncludesTheResource(
            @Open GraknGraph graph, @FromGraph Resource<Object> resource) {
        Object resourceValue = resource.getValue();

        Collection<Resource<Object>> expectedResources = graph.getResourcesByValue(resourceValue);
        resource.delete();
        Collection<Resource<Object>> resourcesAfter = graph.getResourcesByValue(resourceValue);

        expectedResources.remove(resource);

        assertEquals(expectedResources, resourcesAfter);
    }

    @Property
    public void whenCallingGetResourcesByValue_TheResultIsAllResourcesWithTheGivenValue(
            @Open GraknGraph graph, @From(ResourceValues.class) Object resourceValue) {
        Collection<Resource<?>> allResources = graph.admin().getMetaResourceType().instances();

        Set<Resource<?>> allResourcesOfValue =
                allResources.stream().filter(resource -> resourceValue.equals(resource.getValue())).collect(toSet());

        assertEquals(allResourcesOfValue, graph.getResourcesByValue(resourceValue));
    }

    @Property
    public void whenCallingGetResourcesByValueWithAnUnsupportedDataType_Throw(@Open GraknGraph graph, List value) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.unsupportedDataType(value).getMessage());
        //noinspection ResultOfMethodCallIgnored
        graph.getResourcesByValue(value);
    }

    @Property
    public void whenCallingGetEntityType_TheResultIsTheSameAsGetOntologyConcept(
            @Open GraknGraph graph, @FromGraph EntityType type) {
        Label label = type.getLabel();
        assertSameResult(() -> graph.getOntologyConcept(label), () -> graph.getEntityType(label.getValue()));
    }

    @Property
    public void whenCallingGetRelationType_TheResultIsTheSameAsGetOntologyConcept(
            @Open GraknGraph graph, @FromGraph RelationType type) {
        Label label = type.getLabel();
        assertSameResult(() -> graph.getOntologyConcept(label), () -> graph.getRelationType(label.getValue()));
    }

    @Property
    public void whenCallingGetResourceType_TheResultIsTheSameAsGetOntologyConcept(
            @Open GraknGraph graph, @FromGraph ResourceType type) {
        Label label = type.getLabel();
        assertSameResult(() -> graph.getOntologyConcept(label), () -> graph.getResourceType(label.getValue()));
    }

    @Property
    public void whenCallingGetRole_TheResultIsTheSameAsGetOntologyConcept(
            @Open GraknGraph graph, @FromGraph Role role) {
        Label label = role.getLabel();
        assertSameResult(() -> graph.getOntologyConcept(label), () -> graph.getRole(label.getValue()));
    }

    @Property
    public void whenCallingGetRuleType_TheResultIsTheSameAsGetOntologyConcept(
            @Open GraknGraph graph, @FromGraph RuleType type) {
        Label label = type.getLabel();
        assertSameResult(() -> graph.getOntologyConcept(label), () -> graph.getRuleType(label.getValue()));
    }

    @Property
    public void whenCallingAdmin_TheResultIsTheSameGraph(GraknGraph graph) {
        assertEquals(graph, graph.admin());
    }

    @Property
    public void whenCallingClear_TheGraphCloses(@Open GraknGraph graph) {
        graph.admin().delete();
        assertTrue(graph.isClosed());
    }

    @Property
    public void whenCallingClear_OnlyMetaConceptsArePresent(@Open GraknGraph graph) {
        graph.admin().delete();
        graph = Grakn.session(Grakn.IN_MEMORY, graph.getKeyspace()).open(GraknTxType.WRITE);
        List<Concept> concepts = allConceptsFrom(graph);
        concepts.forEach(concept -> {
            assertTrue(concept.isOntologyConcept());
            assertTrue(isMetaLabel(concept.asOntologyConcept().getLabel()));
            });
        graph.close();
    }

    @Property
    public void whenCallingDeleteAndReOpening_AllMetaConceptsArePresent(@Open GraknGraph graph, @From(MetaLabels.class) Label label) {
        graph.admin().delete();
        graph = Grakn.session(Grakn.IN_MEMORY, graph.getKeyspace()).open(GraknTxType.WRITE);
        assertNotNull(graph.getOntologyConcept(label));
        graph.close();
    }

    @Property
    public void whenCallingIsClosedOnAClosedGraph_ReturnTrue(@Open(false) GraknGraph graph) {
        assertTrue(graph.isClosed());
    }

    @Property
    public void whenCallingIsClosedOnAnOpenGraph_ReturnFalse(@Open GraknGraph graph) {
        assertFalse(graph.isClosed());
    }

    @Property
    public void whenCallingClose_TheGraphIsClosed(GraknGraph graph) throws InvalidGraphException {
        graph.close();
        assertTrue(graph.isClosed());
    }


    // TODO: Everything below this point should be moved to more appropriate test classes

    @Property
    public void whenSetRegexOnMetaResourceType_Throw(@Open GraknGraph graph, String regex) {
        ResourceType resource = graph.admin().getMetaResourceType();

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.cannotSetRegex(resource).getMessage());

        resource.setRegex(regex);
    }

    @Property
    public void whenCreateInstanceOfMetaResourceType_Throw(
            @Open GraknGraph graph, @From(ResourceValues.class) Object value) {
        ResourceType resource = graph.admin().getMetaResourceType();

        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.metaTypeImmutable(resource.getLabel()).getMessage());

        resource.putResource(value);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingSuperTypeOnMetaResourceType_Throw(@Open GraknGraph graph) {
        ResourceType resource = graph.admin().getMetaResourceType();

        // TODO: Test for a better error message
        exception.expect(GraphOperationException.class);

        //noinspection ResultOfMethodCallIgnored
        resource.sup();
    }

    @Property
    public void whenCallingHasWithMetaResourceType_ThrowMetaTypeImmutableException(
            @Open GraknGraph graph, @FromGraph Type type) {
        ResourceType resource = graph.admin().getMetaResourceType();

        exception.expect(GraphOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())) {
            exception.expectMessage(GraphOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        } else {
            exception.expectMessage(GraphOperationException.metaTypeImmutable(resource.getLabel()).getMessage());
        }
        type.resource(resource);
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
