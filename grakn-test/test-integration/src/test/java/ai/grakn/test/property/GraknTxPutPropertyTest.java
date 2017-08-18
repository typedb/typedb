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

import ai.grakn.GraknTx;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.generator.Labels.Unused;
import ai.grakn.generator.PutSchemaConceptFunctions;
import ai.grakn.generator.PutTypeFunctions;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.function.BiFunction;

import static ai.grakn.util.Schema.MetaSchema.isMetaLabel;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class GraknTxPutPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenCallingAnyPutSchemaConceptMethod_CreateAnOntologyConceptWithTheGivenLabel(
            @Open GraknTx graph, @Unused Label label,
            @From(PutSchemaConceptFunctions.class) BiFunction<GraknTx, Label, SchemaConcept> putSchemaConcept
    ) {
        SchemaConcept type = putSchemaConcept.apply(graph, label);
        assertEquals(label, type.getLabel());
    }

    @Property
    public void whenCallingAnyPutSchemaConceptMethod_CreateAnOntologyConceptWithDefaultProperties(
            @Open GraknTx graph, @Unused Label label,
            @From(PutSchemaConceptFunctions.class) BiFunction<GraknTx, Label, SchemaConcept> putSchemaConcept
    ) {
        SchemaConcept concept = putSchemaConcept.apply(graph, label);

        assertThat("Concept should only have one sub-type: itself", concept.subs().collect(toSet()), contains(concept));
        assertFalse("Concept should not be implicit", concept.isImplicit());
        assertThat("Rules of hypotheses should be empty", concept.getRulesOfHypothesis().collect(toSet()), empty());
        assertThat("Rules of conclusion should be empty", concept.getRulesOfConclusion().collect(toSet()), empty());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithDefaultProperties(
            @Open GraknTx graph, @Unused Label label,
            @From(PutTypeFunctions.class) BiFunction<GraknTx, Label, Type> putType
    ) {
        Type type = putType.apply(graph, label);

        assertThat("Type should not play any roles", type.plays().collect(toSet()), empty());
        assertThat("Type should not have any scopes", type.scopes().collect(toSet()), empty());
        assertFalse("Type should not be abstract", type.isAbstract());
    }

    @Property
    public void whenCallingPutEntityType_CreateATypeWithSuperTypeEntity(
            @Open GraknTx graph, @Unused Label label) {
        EntityType entityType = graph.putEntityType(label);
        assertEquals(graph.admin().getMetaEntityType(), entityType.sup());
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingEntityTypeLabel_ItReturnsThatType(
            @Open GraknTx graph, @FromGraph EntityType entityType) {
        EntityType newType = graph.putEntityType(entityType.getLabel());
        assertEquals(entityType, newType);
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingNonEntityTypeLabel_Throw(
            @Open GraknTx graph, @FromGraph Type type) {
        assumeFalse(type.isEntityType());

        exception.expect(GraknTxOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.SCHEMA_LABEL, type.getLabel()).getMessage());
        }
        graph.putEntityType(type.getLabel());
    }

    @Property
    public void whenCallingPutResourceType_CreateATypeWithSuperTypeResource(
            @Open GraknTx graph, @Unused Label label, AttributeType.DataType<?> dataType) {
        AttributeType<?> attributeType = graph.putAttributeType(label, dataType);
        assertEquals(graph.admin().getMetaResourceType(), attributeType.sup());
    }

    @Property
    public void whenCallingPutResourceType_CreateATypeWithDefaultProperties(
            @Open GraknTx graph, @Unused Label label, AttributeType.DataType<?> dataType) {
        AttributeType<?> attributeType = graph.putAttributeType(label, dataType);

        assertEquals("The data-type should be as specified", dataType, attributeType.getDataType());
        assertNull("The resource type should have no regex constraint", attributeType.getRegex());
    }

    @Property
    public void whenCallingPutResourceTypeWithThePropertiesOfAnExistingResourceType_ItReturnsThatType(
            @Open GraknTx graph, @FromGraph AttributeType<?> attributeType) {
        assumeFalse(attributeType.equals(graph.admin().getMetaResourceType()));

        Label label = attributeType.getLabel();
        AttributeType.DataType<?> dataType = attributeType.getDataType();

        AttributeType<?> newType = graph.putAttributeType(label, dataType);

        assertEquals(attributeType, newType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonResourceTypeLabel_Throw(
            @Open GraknTx graph, @FromGraph Type type, AttributeType.DataType<?> dataType) {
        assumeFalse(type.isAttributeType());

        exception.expect(GraknTxOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.SCHEMA_LABEL, type.getLabel()).getMessage());
        }
        graph.putAttributeType(type.getLabel(), dataType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonUniqueResourceTypeLabelButADifferentDataType_Throw(
            @Open GraknTx graph, @FromGraph AttributeType<?> attributeType,
            AttributeType.DataType<?> dataType) {
        assumeThat(dataType, not(is(attributeType.getDataType())));
        Label label = attributeType.getLabel();

        exception.expect(GraknTxOperationException.class);
        if(isMetaLabel(label)) {
            exception.expectMessage(GraknTxOperationException.metaTypeImmutable(label).getMessage());
        } else {
            exception.expectMessage(GraknTxOperationException.immutableProperty(attributeType.getDataType(), dataType, Schema.VertexProperty.DATA_TYPE).getMessage());
        }

        graph.putAttributeType(label, dataType);
    }

    @Property
    public void whenCallingPutRuleType_CreateATypeWithSuperTypeRule(@Open GraknTx graph, @Unused Label label) {
        RuleType ruleType = graph.putRuleType(label);
        assertEquals(graph.admin().getMetaRuleType(), ruleType.sup());
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingRuleTypeLabel_ItReturnsThatType(
            @Open GraknTx graph, @FromGraph RuleType ruleType) {
        RuleType newType = graph.putRuleType(ruleType.getLabel());
        assertEquals(ruleType, newType);
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingNonRuleTypeLabel_Throw(
            @Open GraknTx graph, @FromGraph Type type) {
        assumeFalse(type.isRuleType());

        exception.expect(GraknTxOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.SCHEMA_LABEL, type.getLabel()).getMessage());
        }

        graph.putRuleType(type.getLabel());
    }

    @Property
    public void whenCallingPutRelationType_CreateATypeWithSuperTypeRelation(
            @Open GraknTx graph, @Unused Label label) {
        RelationshipType relationshipType = graph.putRelationshipType(label);
        assertEquals(graph.admin().getMetaRelationType(), relationshipType.sup());
    }

    @Property
    public void whenCallingPutRelationType_CreateATypeThatOwnsNoRoles(
            @Open GraknTx graph, @Unused Label label) {
        RelationshipType relationshipType = graph.putRelationshipType(label);
        assertThat(relationshipType.relates().collect(toSet()), empty());
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingRelationTypeLabel_ItReturnsThatType(
            @Open GraknTx graph, @FromGraph RelationshipType relationshipType) {
        RelationshipType newType = graph.putRelationshipType(relationshipType.getLabel());
        assertEquals(relationshipType, newType);
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingNonRelationTypeLabel_Throw(
            @Open GraknTx graph, @FromGraph Type type) {
        assumeFalse(type.isRelationshipType());

        exception.expect(GraknTxOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.SCHEMA_LABEL, type.getLabel()).getMessage());
        }
        graph.putRelationshipType(type.getLabel());
    }

    @Property
    public void whenCallingPutRole_CreateATypeWithSuperRole(@Open GraknTx graph, @Unused Label label) {
        Role role = graph.putRole(label);
        assertEquals(graph.admin().getMetaRole(), role.sup());
    }

    @Property
    public void whenCallingPutRole_CreateARoleWithDefaultProperties(
            @Open GraknTx graph, @Unused Label label) {
        Role role = graph.putRole(label);

        assertThat("The role should be played by no types", role.playedByTypes().collect(toSet()), empty());
        assertThat("The role should be owned by no relation types", role.relationTypes().collect(toSet()), empty());
    }

    @Property
    public void whenCallingPutRoleWithAnExistingRoleLabel_ItReturnsThatRole(
            @Open GraknTx graph, @FromGraph Role role) {
        Role newType = graph.putRole(role.getLabel());
        assertEquals(role, newType);
    }

    @Property
    public void whenCallingPutRoleWithAnExistingTypeLabel_Throw(
            @Open GraknTx graph, @FromGraph Type type) {
        exception.expect(GraknTxOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.SCHEMA_LABEL, type.getLabel()).getMessage());
        }
        graph.putRole(type.getLabel());
    }
}