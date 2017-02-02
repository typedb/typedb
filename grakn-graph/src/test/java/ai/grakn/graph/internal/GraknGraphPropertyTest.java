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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.generator.ResourceValues;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Lists;
import ai.grakn.util.Schema;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnitQuickcheck.class)
public class GraknGraphPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenCallingPutEntityTypeAndTheGraphIsClosedThenThrow(GraknGraph graph, TypeName typeName) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.putEntityType(typeName);
    }

    @Property
    public void whenCallingPutEntityTypeThenCreateATypeWithTheGivenName(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());

        EntityType entityType = graph.putEntityType(typeName);

        assertEquals(typeName, entityType.getName());
    }

    @Property
    public void whenCallingPutEntityTypeThenCreateATypeWithSuperTypeEntity(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        EntityType entityType = graph.putEntityType(typeName);

        assertEquals(graph.admin().getMetaEntityType(), entityType.superType());
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingEntityTypeNameThenItReturnsThatType(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        EntityType entityType = anySubTypeOf(graph.admin().getMetaEntityType());

        EntityType newType = graph.putEntityType(entityType.getName());

        assertEquals(entityType, newType);
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingNonEntityTypeNameThenThrow(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        TypeName typeName = anyTypeNameExcept(graph, Type::isEntityType);

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(typeName, graph.getType(typeName)));

        graph.putEntityType(typeName);
    }

    @Property
    public void whenCallingPutResourceTypeAndTheGraphIsClosedThenThrow(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.putResourceType(typeName, dataType);
    }

    @Property
    public void whenCallingPutResourceTypeThenCreateATypeWithTheGivenName(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());

        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);

        assertEquals(typeName, resourceType.getName());
    }

    @Property
    public void whenCallingPutResourceTypeThenCreateATypeWithSuperTypeResource(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());

        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);

        assertEquals(graph.admin().getMetaResourceType(), resourceType.superType());
    }

    @Property
    public void whenCallingPutResourceTypeThenCreateATypeWithTheGivenDataType(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);

        assertEquals(dataType, resourceType.getDataType());
    }

    @Property
    public void whenCallingPutResourceTypeThenTheResultingTypeIsNotUnique(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);

        assertFalse(resourceType.isUnique());
    }

    @Property
    public void whenCallingPutResourceTypeWithThePropertiesOfAnExistingNonUniqueResourceTypeThenItReturnsThatType(
            GraknGraph graph) {
        assumeFalse(graph.isClosed());
        ResourceType<?> resourceType = nonUniqueResourceTypeFrom(graph);
        TypeName typeName = resourceType.getName();

        assumeFalse(resourceType.equals(graph.admin().getMetaResourceType()));

        ResourceType.DataType<?> dataType = resourceType.getDataType();

        ResourceType<?> newType = graph.putResourceType(typeName, dataType);

        assertEquals(resourceType, newType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonResourceTypeNameThenThrow(
            GraknGraph graph, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        TypeName typeName = anyTypeNameExcept(graph, Type::isResourceType);

        // TODO: Refine the kind of error expected
        exception.expect(GraphRuntimeException.class);

        graph.putResourceType(typeName, dataType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonUniqueResourceTypeNameButADifferentDataTypeThenThrow(
            GraknGraph graph, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        ResourceType<?> resourceType = nonUniqueResourceTypeFrom(graph);

        TypeName typeName = resourceType.getName();
        assumeThat(dataType, not(is(resourceType.getDataType())));

        exception.expect(ConceptException.class);
        if(Schema.MetaSchema.isMetaName(typeName)) {
            exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(typeName));
        } else {
            exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(resourceType.getDataType(), resourceType, dataType, Schema.ConceptProperty.DATA_TYPE.name()));
        }

        graph.putResourceType(typeName, dataType);
    }

    @Property
    public void whenCallingPutResourceTypeUniqueAndTheGraphIsClosedThenThrow(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.putResourceTypeUnique(typeName, dataType);
    }

    @Property
    public void whenCallingPutResourceTypeUniqueThenCreateATypeWithTheGivenName(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);

        assertEquals(typeName, resourceType.getName());
    }

    @Property
    public void whenCallingPutResourceTypeUniqueThenCreateATypeWithSuperTypeResource(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);

        assertEquals(graph.admin().getMetaResourceType(), resourceType.superType());
    }

    @Property
    public void whenCallingPutResourceTypeUniqueThenCreateATypeWithTheGivenDataType(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);

        assertEquals(dataType, resourceType.getDataType());
    }

    @Property
    public void whenCallingPutResourceTypeUniqueThenTheResultingTypeIsUnique(
            GraknGraph graph, TypeName typeName, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);

        assertTrue(resourceType.isUnique());
    }

    @Property
    public void whenCallingPutResourceTypeUniqueWithThePropertiesOfAnExistingUniqueResourceTypeThenItReturnsThatType(
            GraknGraph graph) {
        assumeFalse(graph.isClosed());
        ResourceType<?> resourceType = assumePresent(uniqueResourceTypeFrom(graph));
        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        ResourceType<?> newType = graph.putResourceTypeUnique(typeName, dataType);

        assertEquals(resourceType, newType);
    }

    @Property
    public void whenCallingPutResourceTypeUniqueWithAnExistingNonResourceTypeNameThenThrow(
            GraknGraph graph, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        TypeName typeName = anyTypeNameExcept(graph, Type::isResourceType);

        // TODO: Refine the kind of error expected
        exception.expect(GraphRuntimeException.class);

        graph.putResourceTypeUnique(typeName, dataType);
    }

    @Property
    public void whenCallingPutResourceTypeUniqueWithAnExistingUniqueResourceTypeNameButADifferentDataTypeThenThrow(
            GraknGraph graph, ResourceType.DataType<?> dataType) {
        assumeFalse(graph.isClosed());
        ResourceType<?> resourceType = assumePresent(uniqueResourceTypeFrom(graph));
        TypeName typeName = resourceType.getName();
        assumeThat(dataType, not(is(resourceType.getDataType())));

        // TODO: Refine the kind of error expected
        exception.expect(GraphRuntimeException.class);

        graph.putResourceTypeUnique(typeName, dataType);
    }

    @Property
    public void whenCallingPutRuleTypeAndTheGraphIsClosedThenThrow(GraknGraph graph, TypeName typeName) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.putRuleType(typeName);
    }

    @Property
    public void whenCallingPutRuleTypeThenCreateATypeWithTheGivenName(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());

        RuleType ruleType = graph.putRuleType(typeName);

        assertEquals(typeName, ruleType.getName());
    }

    @Property
    public void whenCallingPutRuleTypeThenCreateATypeWithSuperTypeRule(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        RuleType ruleType = graph.putRuleType(typeName);

        assertEquals(graph.admin().getMetaRuleType(), ruleType.superType());
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingRuleTypeNameThenItReturnsThatType(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        RuleType ruleType = anySubTypeOf(graph.admin().getMetaRuleType());

        RuleType newType = graph.putRuleType(ruleType.getName());

        assertEquals(ruleType, newType);
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingNonRuleTypeNameThenThrow(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        TypeName typeName = anyTypeNameExcept(graph, Type::isRuleType);

        exception.expect(ConceptNotUniqueException.class);
        exception.expectMessage(ErrorMessage.ID_ALREADY_TAKEN.getMessage(typeName, graph.getType(typeName)));

        graph.putRuleType(typeName);
    }

    @Property
    public void whenCallingPutRelationTypeAndTheGraphIsClosedThenThrow(GraknGraph graph, TypeName typeName) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.putRelationType(typeName);
    }

    @Property
    public void whenCallingPutRelationTypeThenCreateATypeWithTheGivenName(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        RelationType relationType = graph.putRelationType(typeName);

        assertEquals(typeName, relationType.getName());
    }

    @Property
    public void whenCallingPutRelationTypeThenCreateATypeWithSuperTypeRelation(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());

        RelationType relationType = graph.putRelationType(typeName);

        assertEquals(graph.admin().getMetaRelationType(), relationType.superType());
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingRelationTypeNameThenItReturnsThatType(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        RelationType relationType = anySubTypeOf(graph.admin().getMetaRelationType());

        RelationType newType = graph.putRelationType(relationType.getName());

        assertEquals(relationType, newType);
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingNonRelationTypeNameThenThrow(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        TypeName typeName = anyTypeNameExcept(graph, Type::isRelationType);

        // TODO: Refine the kind of error expected
        exception.expect(GraphRuntimeException.class);

        graph.putRelationType(typeName);
    }

    @Property
    public void whenCallingPutRoleTypeAndTheGraphIsClosedThenThrow(GraknGraph graph, TypeName typeName) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.putRoleType(typeName);
    }

    @Property
    public void whenCallingPutRoleTypeThenCreateATypeWithTheGivenName(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        RoleType roleType = graph.putRoleType(typeName);

        assertEquals(typeName, roleType.getName());
    }

    @Property
    public void whenCallingPutRoleTypeThenCreateATypeWithSuperTypeRole(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());
        assumeFalse(typeNameExists(graph, typeName));

        RoleType roleType = graph.putRoleType(typeName);

        assertEquals(graph.admin().getMetaRoleType(), roleType.superType());
    }

    @Property
    public void whenCallingPutRoleTypeWithAnExistingRoleTypeNameThenItReturnsThatType(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        RoleType roleType = anySubTypeOf(graph.admin().getMetaRoleType());

        RoleType newType = graph.putRoleType(roleType.getName());

        assertEquals(roleType, newType);
    }

    @Property
    public void whenCallingPutRoleTypeWithAnExistingNonRoleTypeNameThenThrow(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        TypeName typeName = anyTypeNameExcept(graph, Type::isRoleType);

        // TODO: Refine the kind of error expected
        exception.expect(GraphRuntimeException.class);

        graph.putRoleType(typeName);
    }

    @Property
    public void whenCallingGetConceptAndTheGraphIsClosedThenThrow(GraknGraph graph, ConceptId id) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.getConcept(id);
    }

    @Property
    public void whenCallingGetConceptWithAnExistingConceptIdThenItReturnsThatConcept(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        Concept concept = anyConceptFrom(graph);
        ConceptId id = concept.getId();

        assertEquals(concept, graph.getConcept(id));
    }

    @Property
    public void whenCallingGetConceptWithANonExistingConceptIdThenItReturnsNull(GraknGraph graph, ConceptId id) {
        assumeFalse(graph.isClosed());
        Set<ConceptId> allIds = allConceptsFrom(graph).stream().map(Concept::getId).collect(toSet());
        assumeThat(allIds, not(hasItem(id)));

        assertNull(graph.getConcept(id));
    }

    @Property
    public void whenCallingGetConceptWithAnIncorrectGenericThenItThrows(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        Concept concept = anyConceptFrom(graph);
        assumeFalse(concept.isRoleType());
        ConceptId id = concept.getId();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        RoleType roleType = graph.getConcept(id);
    }

    @Property
    public void whenCallingGetTypeAndTheGraphIsClosedThenThrow(GraknGraph graph, TypeName typeName) {
        assumeTrue(graph.isClosed());

        exception.expect(GraphRuntimeException.class);
        exception.expectMessage(ErrorMessage.CLOSED_USER.getMessage());

        graph.getType(typeName);
    }

    @Property
    public void whenCallingGetTypeWithAnExistingTypeNameThenItReturnsThatType(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        Type type = anyTypeFrom(graph);
        TypeName typeName = type.getName();

        assertEquals(type, graph.getType(typeName));
    }


    @Property
    public void whenCallingGetTypeWithANonExistingTypeNameThenItReturnsNull(GraknGraph graph, TypeName typeName) {
        assumeFalse(graph.isClosed());
        Set<TypeName> allTypes = allTypesFrom(graph).stream().map(Type::getName).collect(toSet());
        assumeThat(allTypes, not(hasItem(typeName)));

        assertNull(graph.getType(typeName));
    }

    @Property
    public void whenCallingGetTypeWithAnIncorrectGenericThenItThrows(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        Type type = anyTypeFrom(graph);
        assumeFalse(type.isRoleType());
        TypeName typeName = type.getName();

        exception.expect(ClassCastException.class);

        // We have to assign the result for the cast to happen
        //noinspection unused
        RoleType roleType = graph.getType(typeName);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenDeletingMetaEntityTypeThenThrow(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        EntityType entity = graph.admin().getMetaEntityType();

        exception.expect(ConceptException.class);
        exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(entity.getName()));

        entity.delete();
    }

    @Property
    public void whenSetRegexOnMetaResourceTypeThenThrow(GraknGraph graph, String regex) {
        assumeFalse(graph.isClosed());

        ResourceType resource = graph.admin().getMetaResourceType();

        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage(ErrorMessage.REGEX_NOT_STRING.getMessage(resource.getName()));

        resource.setRegex(regex);
    }

    @Property
    public void whenCallingIsUniqueOnMetaResourceTypeThenResultIsFalse(GraknGraph graph) {
        assumeFalse(graph.isClosed());
        ResourceType resource = graph.admin().getMetaResourceType();
        assertFalse(resource.isUnique());
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCreateInstanceOfMetaResourceTypeThenThrow(
            GraknGraph graph, @From(ResourceValues.class) Object value) {
        ResourceType resource = graph.admin().getMetaResourceType();

        // TODO: Test for a better error message
        exception.expect(GraphRuntimeException.class);

        resource.putResource(value);
    }

    @Ignore // TODO: Fix this
    @Property
    public void whenCallingSuperTypeOnMetaResourceTypeThenThrow(GraknGraph graph) {
        ResourceType resource = graph.admin().getMetaResourceType();

        // TODO: Test for a better error message
        exception.expect(GraphRuntimeException.class);

        resource.superType();
    }

    private static boolean typeNameExists(GraknGraph graph, TypeName typeName) {
        return graph.getType(typeName) != null;
    }

    private static Collection<? extends Type> allTypesFrom(GraknGraph graph) {
        return graph.admin().getMetaConcept().subTypes();
    }


    private static List<Concept> allConceptsFrom(GraknGraph graph) {
        List<Concept> concepts = Lists.newArrayList(allTypesFrom(graph));
        concepts.addAll(graph.admin().getMetaConcept().instances());
        return concepts;
    }

    private static Concept anyConceptFrom(GraknGraph graph) {
        List<Concept> concepts = allConceptsFrom(graph);
        int index = new Random().nextInt(concepts.size());
        return concepts.get(index);
    }

    private static ResourceType<?> nonUniqueResourceTypeFrom(GraknGraph graph) {
        return ((Collection<ResourceType>) graph.admin().getMetaResourceType().subTypes()).stream()
                .filter(resourceType -> !nullAsFalse(resourceType.isUnique())).findAny().get();
    }

    private static Optional<ResourceType> uniqueResourceTypeFrom(GraknGraph graph) {
        return ((Collection<ResourceType>) graph.admin().getMetaResourceType().subTypes()).stream()
                .filter(resourceType -> nullAsFalse(resourceType.isUnique())).findAny();
    }

    private static TypeName anyTypeNameExcept(GraknGraph graph, Predicate<Type> predicate) {
        return graph.admin().getMetaConcept().subTypes().stream()
                .filter(predicate.negate()).findAny().get().getName();
    }

    private static <T extends Type> T anySubTypeOf(T type) {
        return (T) type.subTypes().stream().findAny().get();
    }

    private static Type anyTypeFrom(GraknGraph graph) {
        return anySubTypeOf(graph.admin().getMetaConcept());
    }

    private static <T> T assumePresent(Optional<T> optional) {
        assumeTrue(optional.isPresent());
        assert optional.isPresent();
        return optional.get();
    }

    // TODO: Remove hacky fix to handle null isUnique property
    private static boolean nullAsFalse(Boolean bool) {
        return bool == null ? false : bool ;
    }
}
