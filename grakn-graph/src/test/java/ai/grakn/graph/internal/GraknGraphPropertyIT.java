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
 *
 */

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.generator.AbstractTypeGenerator.NotMeta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import ai.grakn.generator.MetaTypeNames;
import ai.grakn.generator.Methods.MethodOf;
import ai.grakn.generator.PutTypeFunctions;
import ai.grakn.generator.ResourceTypes.Unique;
import ai.grakn.generator.ResourceValues;
import ai.grakn.generator.TypeNames.Unused;
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
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static ai.grakn.generator.GraknGraphs.allConceptsFrom;
import static ai.grakn.generator.GraknGraphs.allTypesFrom;
import static ai.grakn.generator.Methods.mockParamsOf;
import static ai.grakn.util.Schema.MetaSchema.isMetaName;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class GraknGraphPropertyIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Ignore //TODO: This is breaking because your mocked concepts have null concept IDs this is an impossible state so I think you should get your generater to mock valid concepts
    @Property
    public void whenCallingMostMethodOnAClosedGraph_Throw(
            @Open(false) GraknGraph graph, @MethodOf(GraknGraph.class) Method method) throws Throwable {

        // TODO: Should `admin`, `close`, `implicitConceptsVisible`, `showImplicitConcepts`, `getKeyspace` and `graql` be here?
        assumeThat(method.getName(), not(isOneOf("open", "close", "admin", "isClosed", "implicitConceptsVisible", "showImplicitConcepts", "getKeyspace", "graql")));
        Object[] params = mockParamsOf(method);

        exception.expect(InvocationTargetException.class);
        exception.expectCause(isA(GraphRuntimeException.class));
        exception.expectCause(hasProperty("message", is(ErrorMessage.CLOSED_USER.getMessage())));

        method.invoke(graph, params);
    }

    @Ignore // TODO: Fix this (or remove test)
    @Property
    public void whenCallingAnyMethodWithNull_Throw(
            @Open GraknGraph graph, @MethodOf(GraknGraph.class) Method method) throws Throwable {
        int numParameters = method.getParameterCount();
        assumeThat(numParameters, greaterThan(0));
        Object[] nulls = new Object[numParameters];

        exception.expect(InvocationTargetException.class);
        exception.expectCause(isA(NullPointerException.class));

        method.invoke(graph, nulls);
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithTheGivenName(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        assertEquals(typeName, type.getName());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithoutSubTypes(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        graph.showImplicitConcepts(true);
        assertThat(type.subTypes(), contains(type));
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeNotPlayingAnyRoles(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        graph.showImplicitConcepts(true);
        assertThat(type.playsRoles(), empty());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateANonAbstractType(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        assertFalse(type.isAbstract());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateANonImplicitType(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        assertFalse(type.isImplicit());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithoutHypotheses(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        assertThat(type.getRulesOfHypothesis(), empty());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithoutConclusions(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        assertThat(type.getRulesOfConclusion(), empty());
    }

    @Property
    public void whenCallingPutEntityType_CreateATypeWithSuperTypeEntity(
            @Open GraknGraph graph, @Unused TypeName typeName) {
        EntityType entityType = graph.putEntityType(typeName);
        assertEquals(graph.admin().getMetaEntityType(), entityType.superType());
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingEntityTypeName_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph EntityType entityType) {
        EntityType newType = graph.putEntityType(entityType.getName());
        assertEquals(entityType, newType);
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingNonEntityTypeName_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(type.isEntityType());

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(type.getName(), type));

        graph.putEntityType(type.getName());
    }

    @Property
    public void whenCallingPutResourceType_CreateATypeWithSuperTypeResource(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);
        assertEquals(graph.admin().getMetaResourceType(), resourceType.superType());
    }

    @Property
    public void whenCallingPutResourceType_CreateATypeWithTheGivenDataType(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);
        assertEquals(dataType, resourceType.getDataType());
    }

    @Property
    public void whenCallingPutResourceType_TheResultingTypeIsNotUnique(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);
        assertFalse(resourceType.isUnique());
    }

    @Property
    public void whenCallingPutResourceType_TheResultingTypeHasNoRegexConstraint(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);
        assertNull(resourceType.getRegex());
    }

    @Property
    public void whenCallingPutResourceTypeWithThePropertiesOfAnExistingNonUniqueResourceType_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph @Unique(false) ResourceType<?> resourceType) {
        assumeFalse(resourceType.equals(graph.admin().getMetaResourceType()));

        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        ResourceType<?> newType = graph.putResourceType(typeName, dataType);

        assertEquals(resourceType, newType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonResourceTypeName_Throw(
            @Open GraknGraph graph, @FromGraph Type type, ResourceType.DataType<?> dataType) {
        assumeFalse(type.isResourceType());

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(type.getName(), type));

        graph.putResourceType(type.getName(), dataType);
    }

    @Ignore //TODO: Caching has broken this test because the data type can be passed in. As the datatype is under debate to be embedded in the ontology I will hold off on fixing fro the moment
    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonUniqueResourceTypeNameButADifferentDataType_Throw(
            @Open GraknGraph graph, @FromGraph @Unique(false) ResourceType<?> resourceType,
            ResourceType.DataType<?> dataType) {
        assumeThat(dataType, not(is(resourceType.getDataType())));
        TypeName typeName = resourceType.getName();

        exception.expect(ConceptException.class);
        if(isMetaName(typeName)) {
            exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(typeName));
        } else {
            exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(resourceType.getDataType(), resourceType, dataType, Schema.ConceptProperty.DATA_TYPE.name()));
        }

        graph.putResourceType(typeName, dataType);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingPutResourceTypeWithAnExistingUniqueResourceTypeName_Throw(
            @Open GraknGraph graph, @FromGraph @Unique ResourceType<?> resourceType) {
        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        exception.expect(ConceptException.class);
        if(isMetaName(typeName)) {
            exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(typeName));
        } else {
            exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(resourceType.getDataType(), resourceType, dataType, Schema.ConceptProperty.DATA_TYPE.name()));
        }

        graph.putResourceType(typeName, dataType);
    }

    @Property
    public void whenCallingPutResourceTypeUnique_CreateATypeWithSuperTypeResource(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);
        assertEquals(graph.admin().getMetaResourceType(), resourceType.superType());
    }

    @Property
    public void whenCallingPutResourceTypeUnique_CreateATypeWithTheGivenDataType(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);
        assertEquals(dataType, resourceType.getDataType());
    }

    @Property
    public void whenCallingPutResourceTypeUnique_TheResultingTypeIsUnique(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);
        assertTrue(resourceType.isUnique());
    }

    @Property
    public void whenCallingPutResourceTypeUnique_TheResultingTypeHasNoRegexConstraint(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);
        assertNull(resourceType.getRegex());
    }

    @Property
    public void whenCallingPutResourceTypeUniqueWithThePropertiesOfAnExistingUniqueResourceType_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph @Unique ResourceType<?> resourceType) {
        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        ResourceType<?> newType = graph.putResourceTypeUnique(typeName, dataType);

        assertEquals(resourceType, newType);
    }

    @Property
    public void whenCallingPutResourceTypeUniqueWithAnExistingNonResourceTypeName_Throw(
            @Open GraknGraph graph, @FromGraph Type type, ResourceType.DataType<?> dataType) {
        assumeFalse(type.isResourceType());

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(type.getName(), type));

        graph.putResourceTypeUnique(type.getName(), dataType);
    }

    @Ignore //TODO: Caching has broken this test because the data type can be passed in. As the datatype is under debate to be embedded in the ontology I will hold off on fixing fro the moment
    @Property
    public void whenCallingPutResourceTypeUniqueWithAnExistingUniqueResourceTypeNameButADifferentDataType_Throw(
            @Open GraknGraph graph, @FromGraph @Unique ResourceType<?> resourceType,
            ResourceType.DataType<?> dataType) {
        assumeThat(dataType, not(is(resourceType.getDataType())));
        TypeName typeName = resourceType.getName();

        exception.expect(InvalidConceptValueException.class);
        exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(resourceType.getDataType().getName(), resourceType, dataType.getName(), Schema.ConceptProperty.DATA_TYPE.name()));

        graph.putResourceTypeUnique(typeName, dataType);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingPutResourceTypeUniqueWithAnExistingNonUniqueResourceTypeName_Throw(
            @Open GraknGraph graph, @FromGraph @Unique(false) ResourceType<?> resourceType) {
        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        exception.expect(ConceptException.class);
        if(isMetaName(typeName)) {
            exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(typeName));
        } else {
            exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(resourceType.getDataType(), resourceType, dataType, Schema.ConceptProperty.DATA_TYPE.name()));
        }

        graph.putResourceTypeUnique(typeName, dataType);
    }

    @Property
    public void whenCallingPutRuleType_CreateATypeWithSuperTypeRule(@Open GraknGraph graph, @Unused TypeName typeName) {
        RuleType ruleType = graph.putRuleType(typeName);
        assertEquals(graph.admin().getMetaRuleType(), ruleType.superType());
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingRuleTypeName_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph RuleType ruleType) {
        RuleType newType = graph.putRuleType(ruleType.getName());
        assertEquals(ruleType, newType);
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingNonRuleTypeName_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(type.isRuleType());

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(type.getName(), type));

        graph.putRuleType(type.getName());
    }

    @Property
    public void whenCallingPutRelationType_CreateATypeWithSuperTypeRelation(
            @Open GraknGraph graph, @Unused TypeName typeName) {
        RelationType relationType = graph.putRelationType(typeName);
        assertEquals(graph.admin().getMetaRelationType(), relationType.superType());
    }

    @Property
    public void whenCallingPutRelationType_CreateATypeThatOwnsNoRoles(
            @Open GraknGraph graph, @Unused TypeName typeName) {
        RelationType relationType = graph.putRelationType(typeName);
        graph.showImplicitConcepts(true);
        assertThat(relationType.hasRoles(), empty());
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingRelationTypeName_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph RelationType relationType) {
        RelationType newType = graph.putRelationType(relationType.getName());
        assertEquals(relationType, newType);
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingNonRelationTypeName_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(type.isRelationType());

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(type.getName(), type));

        graph.putRelationType(type.getName());
    }

    @Property
    public void whenCallingPutRoleType_CreateATypeWithSuperTypeRole(@Open GraknGraph graph, @Unused TypeName typeName) {
        RoleType roleType = graph.putRoleType(typeName);
        assertEquals(graph.admin().getMetaRoleType(), roleType.superType());
    }

    @Property
    public void whenCallingPutRoleType_CreateATypePlayedByNoTypes(@Open GraknGraph graph, @Unused TypeName typeName) {
        RoleType roleType = graph.putRoleType(typeName);
        assertThat(roleType.playedByTypes(), empty());
    }

    @Property
    public void whenCallingPutRoleType_CreateATypeOwnedByNoRelationTypes(
            @Open GraknGraph graph, @Unused TypeName typeName) {
        RoleType roleType = graph.putRoleType(typeName);
        assertThat(roleType.relationTypes(), empty());
    }

    @Property
    public void whenCallingPutRoleTypeWithAnExistingRoleTypeName_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph RoleType roleType) {
        RoleType newType = graph.putRoleType(roleType.getName());
        assertEquals(roleType, newType);
    }

    @Property
    public void whenCallingPutRoleTypeWithAnExistingNonRoleTypeName_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(type.isRoleType());

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(type.getName(), type));

        graph.putRoleType(type.getName());
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
        assumeFalse(concept.isRoleType());
        ConceptId id = concept.getId();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        RoleType roleType = graph.getConcept(id);
    }

    @Property
    public void whenCallingGetTypeWithAnExistingTypeName_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph Type type) {
        TypeName typeName = type.getName();
        assertEquals(type, graph.getType(typeName));
    }

    @Ignore // TODO: Re-enable this test when issue with types being super type of themselves is resolved
    @Property
    public void whenCallingGetTypeWithANonExistingTypeName_ItReturnsNull(@Open GraknGraph graph, TypeName typeName) {
        Set<TypeName> allTypes = allTypesFrom(graph).stream().map(Type::getName).collect(toSet());
        assumeThat(allTypes, not(hasItem(typeName)));

        assertNull(graph.getType(typeName));
    }

    @Property
    public void whenCallingGetTypeWithAnIncorrectGeneric_ItThrows(@Open GraknGraph graph, @FromGraph Type type) {
        assumeFalse(type.isRoleType());
        TypeName typeName = type.getName();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        RoleType roleType = graph.getType(typeName);
    }

    @Property
    public void whenCallingGetResourcesByValueAfterAddingAResource_TheResultIncludesTheResource(
            @Open GraknGraph graph,
            @FromGraph @NotMeta ResourceType resourceType, @From(ResourceValues.class) Object value) {
        assumeThat(value.getClass().getName(), is(resourceType.getDataType().getName()));

        Collection<Resource<Object>> expectedResources = graph.getResourcesByValue(value);
        Resource resource = resourceType.putResource(value);
        Collection<Resource<Object>> resourcesAfter = graph.getResourcesByValue(value);

        expectedResources.add(resource);

        assertEquals(expectedResources, resourcesAfter);
    }

    @Ignore // TODO: Fix this test
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

    @Ignore // TODO: Fix this test
    @Property
    public void whenCallingGetResourcesByValue_TheResultIsAllResourcesWithTheGivenValue(
            @Open GraknGraph graph, @From(ResourceValues.class) Object resourceValue) {
        Collection<Resource<?>> allResources = graph.admin().getMetaResourceType().instances();

        Set<Resource<?>> allResourcesOfValue =
                allResources.stream().filter(resource -> resource.getValue().equals(resourceValue)).collect(toSet());

        assertEquals(allResourcesOfValue, graph.getResourcesByValue(resourceValue));
    }

    @Ignore // TODO: Fix this test, or agree on distinction between getType and getEntityType
    @Property
    public void whenCallingGetEntityType_TheResultIsTheSameAsGetType(@Open GraknGraph graph, TypeName typeName) {
        assertSameResult(() -> graph.getType(typeName), () -> graph.getEntityType(typeName.getValue()));
    }

    @Ignore // TODO: Fix this test, or agree on distinction between getType and getRelationType
    @Property
    public void whenCallingGetRelationType_TheResultIsTheSameAsGetType(@Open GraknGraph graph, TypeName typeName) {
        assertSameResult(() -> graph.getType(typeName), () -> graph.getRelationType(typeName.getValue()));
    }

    @Ignore // TODO: Fix this test, or agree on distinction between getType and getResourceType
    @Property
    public void whenCallingGetResourceType_TheResultIsTheSameAsGetType(@Open GraknGraph graph, TypeName typeName) {
        assertSameResult(() -> graph.getType(typeName), () -> graph.getResourceType(typeName.getValue()));
    }

    @Ignore // TODO: Fix this test, or agree on distinction between getType and getRoleType
    @Property
    public void whenCallingGetRoleType_TheResultIsTheSameAsGetType(@Open GraknGraph graph, TypeName typeName) {
        assertSameResult(() -> graph.getType(typeName), () -> graph.getRoleType(typeName.getValue()));
    }

    @Ignore // TODO: Fix this test, or agree on distinction between getType and getRuleType
    @Property
    public void whenCallingGetRuleType_TheResultIsTheSameAsGetType(@Open GraknGraph graph, TypeName typeName) {
        assertSameResult(() -> graph.getType(typeName), () -> graph.getRuleType(typeName.getValue()));
    }

    @Ignore // TODO: Fix this test
    @Property
    public void whenCallingGetRelationAndTheRelationExists_ReturnThatRelation(
            @Open GraknGraph graph, @FromGraph Relation relation) {
        assertEquals(relation, graph.getRelation(relation.type(), relation.rolePlayers()));
    }

    @Property
    public void whenCallingGetRelationAndTheRelationDoesntExist_ReturnNull(
            @Open GraknGraph graph,
            @FromGraph RelationType type, Map<@FromGraph RoleType, @FromGraph Instance> roleMap) {
        Collection<Relation> instances = type.instances();
        Set<Map<RoleType, Instance>> roleMaps = instances.stream().map(Relation::rolePlayers).collect(toSet());
        assumeThat(roleMaps, not(hasItem(roleMap)));

        assertNull(graph.getRelation(type, roleMap));
    }

    @Property
    public void whenCallingAdmin_TheResultIsTheSameGraph(GraknGraph graph) {
        assertEquals(graph, graph.admin());
    }

    @Property
    public void whenCallingShowImplicitConcepts_ImplicitConceptsVisibleIsTheSame(GraknGraph graph, boolean flag) {
        graph.showImplicitConcepts(flag);

        assertEquals(flag, graph.implicitConceptsVisible());
    }

    @Property
    public void whenCallingClear_TheGraphCloses(@Open GraknGraph graph) {
        graph.clear();
        assertTrue(graph.isClosed());
    }

    @Ignore // TODO: Re-enable this when test below is fixed
    @Property
    public void whenCallingClear_OnlyMetaConceptsArePresent(@Open GraknGraph graph) {
        graph.clear();
        graph.open();

        List<Concept> concepts = allConceptsFrom(graph);
        concepts.forEach(concept -> {
            assertTrue(concept.isType());
            assertTrue(isMetaName(concept.asType().getName()));
        });
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingClear_AllMetaConceptsArePresent(
            @Open GraknGraph graph, @From(MetaTypeNames.class) TypeName typeName) {
        graph.clear();
        graph.open();
        assertNotNull(graph.getType(typeName));
    }

    @Ignore // TODO: Fix this, or remove the test
    @Property
    public void whenCallingGetKeySpace_ReturnTheKeyspaceOfTheGraph(String keyspace) {
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraph();
        assertEquals(keyspace, graph.getKeyspace());
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
    public void whenCallingClose_TheGraphIsClosed(GraknGraph graph) {
        graph.close();

        assertTrue(graph.isClosed());
    }

    @Property
    public void whenCallingOpen_TheGraphIsOpen(GraknGraph graph) {
        graph.open();

        assertFalse(graph.isClosed());
    }

    // TODO: Everything below this point should be moved to more appropriate test classes

    @Property
    public void whenDeletingMetaEntityType_Throw(@Open GraknGraph graph) {
        EntityType entity = graph.admin().getMetaEntityType();

        exception.expect(ConceptException.class);
        exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(entity.getName()));

        entity.delete();
    }

    @Property
    public void whenSetRegexOnMetaResourceType_Throw(@Open GraknGraph graph, String regex) {
        ResourceType resource = graph.admin().getMetaResourceType();

        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage(ErrorMessage.REGEX_NOT_STRING.getMessage(resource.getName()));

        resource.setRegex(regex);
    }

    @Property
    public void whenCallingIsUniqueOnMetaResourceType_ResultIsFalse(@Open GraknGraph graph) {
        ResourceType resource = graph.admin().getMetaResourceType();
        assertFalse(resource.isUnique());
    }

    @Property
    public void whenCreateInstanceOfMetaResourceType_Throw(
            @Open GraknGraph graph, @From(ResourceValues.class) Object value) {
        ResourceType resource = graph.admin().getMetaResourceType();

        exception.expect(ConceptException.class);
        exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(resource.getName()));

        resource.putResource(value);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingSuperTypeOnMetaResourceType_Throw(@Open GraknGraph graph) {
        ResourceType resource = graph.admin().getMetaResourceType();

        // TODO: Test for a better error message
        exception.expect(GraphRuntimeException.class);

        resource.superType();
    }

    @Ignore // TODO: Fix this and write test properly!
    @Property
    public void whenCallingHasResourceWithMetaResourceType_DontThrowClassCastException(
            @Open GraknGraph graph, @FromGraph Type type) {
        ResourceType resource = graph.admin().getMetaResourceType();

        try {
            type.hasResource(resource);
        } catch (ClassCastException e) {
            fail();
        }
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenGettingSuperType_TheResultIsNeverItself(@Open GraknGraph graph, TypeName typeName) {
        Type type = graph.getType(typeName);
        assumeNotNull(type);
        assertNotEquals(type, type.superType());
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
