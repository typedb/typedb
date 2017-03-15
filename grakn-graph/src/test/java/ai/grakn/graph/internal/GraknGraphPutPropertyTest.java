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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import ai.grakn.generator.PutTypeFunctions;
import ai.grakn.generator.ResourceTypes.Unique;
import ai.grakn.generator.TypeNames.Unused;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.function.BiFunction;

import static ai.grakn.util.Schema.MetaSchema.isMetaName;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class GraknGraphPutPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithTheGivenName(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);
        assertEquals(typeName, type.getName());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithDefaultProperties(
            @Open GraknGraph graph,
            @Unused TypeName typeName, @From(PutTypeFunctions.class) BiFunction<GraknGraph, TypeName, Type> putType) {
        Type type = putType.apply(graph, typeName);

        assertThat("Type should only have one sub-type: itself", type.subTypes(), contains(type));
        assertThat("Type should not play any roles", type.playsRoles(), empty());
        assertFalse("Type should not be abstract", type.isAbstract());
        assertFalse("Type should not be implicit", type.isImplicit());
        assertThat("Rules of hypotheses should be empty", type.getRulesOfHypothesis(), empty());
        assertThat("Rules of conclusion should be empty", type.getRulesOfConclusion(), empty());
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
    public void whenCallingPutResourceType_CreateATypeWithDefaultProperties(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(typeName, dataType);

        assertEquals("The data-type should be as specified", dataType, resourceType.getDataType());
        assertFalse("The resource type should not be unique", resourceType.isUnique());
        assertNull("The resource type should have no regex constraint", resourceType.getRegex());
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

    @Property
    public void whenCallingPutResourceTypeWithAnExistingUniqueResourceTypeName_Throw(
            @Open GraknGraph graph, @FromGraph @Unique ResourceType<?> resourceType) {
        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        exception.expect(ConceptException.class);
        if(isMetaName(typeName)) {
            exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(typeName));
        } else {
            exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(true, resourceType, false, Schema.ConceptProperty.IS_UNIQUE.name()));
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
    public void whenCallingPutResourceTypeUnique_CreateATypeWithDefaultProperties(
            @Open GraknGraph graph, @Unused TypeName typeName, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceTypeUnique(typeName, dataType);

        assertEquals("The data-type should be as specified", dataType, resourceType.getDataType());
        assertTrue("The resource type should be unique", resourceType.isUnique());
        assertNull("The resource type should have no regex constraint", resourceType.getRegex());
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

    @Property
    public void whenCallingPutResourceTypeUniqueWithAnExistingNonUniqueResourceTypeName_Throw(
            @Open GraknGraph graph, @FromGraph @Unique(false) ResourceType<?> resourceType) {
        TypeName typeName = resourceType.getName();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        exception.expect(ConceptException.class);
        if(isMetaName(typeName)) {
            exception.expectMessage(ErrorMessage.META_TYPE_IMMUTABLE.getMessage(typeName));
        } else {
            exception.expectMessage(ErrorMessage.IMMUTABLE_VALUE.getMessage(false, resourceType, true, Schema.ConceptProperty.IS_UNIQUE.name()));
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
    public void whenCallingPutRoleType_CreateATypeWithDefaultProperties(
            @Open GraknGraph graph, @Unused TypeName typeName) {
        RoleType roleType = graph.putRoleType(typeName);

        assertThat("The role type should be played by no types", roleType.playedByTypes(), empty());
        assertThat("The role type should be owned by no relation types", roleType.relationTypes(), empty());
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
}
