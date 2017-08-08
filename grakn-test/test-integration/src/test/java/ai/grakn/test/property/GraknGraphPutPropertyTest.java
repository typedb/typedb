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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import ai.grakn.generator.Labels.Unused;
import ai.grakn.generator.PutOntologyConceptFunctions;
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
public class GraknGraphPutPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenCallingAnyPutOntologyConceptMethod_CreateAnOntologyConceptWithTheGivenLabel(
            @Open GraknGraph graph, @Unused Label label,
            @From(PutOntologyConceptFunctions.class) BiFunction<GraknGraph, Label, OntologyConcept> putOntologyConcept
    ) {
        OntologyConcept type = putOntologyConcept.apply(graph, label);
        Assert.assertEquals(label, type.getLabel());
    }

    @Property
    public void whenCallingAnyPutOntologyConceptMethod_CreateAnOntologyConceptWithDefaultProperties(
            @Open GraknGraph graph, @Unused Label label,
            @From(PutOntologyConceptFunctions.class) BiFunction<GraknGraph, Label, OntologyConcept> putOntologyConcept
    ) {
        OntologyConcept concept = putOntologyConcept.apply(graph, label);

        Assert.assertThat("Concept should only have one sub-type: itself", concept.subs().collect(toSet()), Matchers.contains(concept));
        Assert.assertFalse("Concept should not be implicit", concept.isImplicit());
        Assert.assertThat("Rules of hypotheses should be empty", concept.getRulesOfHypothesis().collect(toSet()), Matchers.empty());
        Assert.assertThat("Rules of conclusion should be empty", concept.getRulesOfConclusion().collect(toSet()), Matchers.empty());
    }

    @Property
    public void whenCallingAnyPutTypeMethod_CreateATypeWithDefaultProperties(
            @Open GraknGraph graph, @Unused Label label,
            @From(PutTypeFunctions.class) BiFunction<GraknGraph, Label, Type> putType
    ) {
        Type type = putType.apply(graph, label);

        Assert.assertThat("Type should not play any roles", type.plays().collect(toSet()), Matchers.empty());
        Assert.assertThat("Type should not have any scopes", type.scopes().collect(toSet()), Matchers.empty());
        Assert.assertFalse("Type should not be abstract", type.isAbstract());
    }

    @Property
    public void whenCallingPutEntityType_CreateATypeWithSuperTypeEntity(
            @Open GraknGraph graph, @Unused Label label) {
        EntityType entityType = graph.putEntityType(label);
        Assert.assertEquals(graph.admin().getMetaEntityType(), entityType.sup());
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingEntityTypeLabel_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph EntityType entityType) {
        EntityType newType = graph.putEntityType(entityType.getLabel());
        Assert.assertEquals(entityType, newType);
    }

    @Property
    public void whenCallingPutEntityTypeWithAnExistingNonEntityTypeLabel_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        Assume.assumeFalse(type.isEntityType());

        exception.expect(GraphOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.ONTOLOGY_LABEL, type.getLabel()).getMessage());
        }
        graph.putEntityType(type.getLabel());
    }

    @Property
    public void whenCallingPutResourceType_CreateATypeWithSuperTypeResource(
            @Open GraknGraph graph, @Unused Label label, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(label, dataType);
        Assert.assertEquals(graph.admin().getMetaResourceType(), resourceType.sup());
    }

    @Property
    public void whenCallingPutResourceType_CreateATypeWithDefaultProperties(
            @Open GraknGraph graph, @Unused Label label, ResourceType.DataType<?> dataType) {
        ResourceType<?> resourceType = graph.putResourceType(label, dataType);

        Assert.assertEquals("The data-type should be as specified", dataType, resourceType.getDataType());
        Assert.assertNull("The resource type should have no regex constraint", resourceType.getRegex());
    }

    @Property
    public void whenCallingPutResourceTypeWithThePropertiesOfAnExistingResourceType_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph  ResourceType<?> resourceType) {
        Assume.assumeFalse(resourceType.equals(graph.admin().getMetaResourceType()));

        Label label = resourceType.getLabel();
        ResourceType.DataType<?> dataType = resourceType.getDataType();

        ResourceType<?> newType = graph.putResourceType(label, dataType);

        Assert.assertEquals(resourceType, newType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonResourceTypeLabel_Throw(
            @Open GraknGraph graph, @FromGraph Type type, ResourceType.DataType<?> dataType) {
        Assume.assumeFalse(type.isResourceType());

        exception.expect(GraphOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.ONTOLOGY_LABEL, type.getLabel()).getMessage());
        }
        graph.putResourceType(type.getLabel(), dataType);
    }

    @Property
    public void whenCallingPutResourceTypeWithAnExistingNonUniqueResourceTypeLabelButADifferentDataType_Throw(
            @Open GraknGraph graph, @FromGraph ResourceType<?> resourceType,
            ResourceType.DataType<?> dataType) {
        Assume.assumeThat(dataType, Matchers.not(Matchers.is(resourceType.getDataType())));
        Label label = resourceType.getLabel();

        exception.expect(GraphOperationException.class);
        if(MetaSchema.isMetaLabel(label)) {
            exception.expectMessage(GraphOperationException.metaTypeImmutable(label).getMessage());
        } else {
            exception.expectMessage(GraphOperationException.immutableProperty(resourceType.getDataType(), dataType, Schema.VertexProperty.DATA_TYPE).getMessage());
        }

        graph.putResourceType(label, dataType);
    }

    @Property
    public void whenCallingPutRuleType_CreateATypeWithSuperTypeRule(@Open GraknGraph graph, @Unused Label label) {
        RuleType ruleType = graph.putRuleType(label);
        Assert.assertEquals(graph.admin().getMetaRuleType(), ruleType.sup());
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingRuleTypeLabel_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph RuleType ruleType) {
        RuleType newType = graph.putRuleType(ruleType.getLabel());
        Assert.assertEquals(ruleType, newType);
    }

    @Property
    public void whenCallingPutRuleTypeWithAnExistingNonRuleTypeLabel_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        Assume.assumeFalse(type.isRuleType());

        exception.expect(GraphOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.ONTOLOGY_LABEL, type.getLabel()).getMessage());
        }

        graph.putRuleType(type.getLabel());
    }

    @Property
    public void whenCallingPutRelationType_CreateATypeWithSuperTypeRelation(
            @Open GraknGraph graph, @Unused Label label) {
        RelationType relationType = graph.putRelationType(label);
        Assert.assertEquals(graph.admin().getMetaRelationType(), relationType.sup());
    }

    @Property
    public void whenCallingPutRelationType_CreateATypeThatOwnsNoRoles(
            @Open GraknGraph graph, @Unused Label label) {
        RelationType relationType = graph.putRelationType(label);
        Assert.assertThat(relationType.relates().collect(toSet()), Matchers.empty());
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingRelationTypeLabel_ItReturnsThatType(
            @Open GraknGraph graph, @FromGraph RelationType relationType) {
        RelationType newType = graph.putRelationType(relationType.getLabel());
        Assert.assertEquals(relationType, newType);
    }

    @Property
    public void whenCallingPutRelationTypeWithAnExistingNonRelationTypeLabel_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        Assume.assumeFalse(type.isRelationType());

        exception.expect(GraphOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.ONTOLOGY_LABEL, type.getLabel()).getMessage());
        }
        graph.putRelationType(type.getLabel());
    }

    @Property
    public void whenCallingPutRole_CreateATypeWithSuperRole(@Open GraknGraph graph, @Unused Label label) {
        Role role = graph.putRole(label);
        Assert.assertEquals(graph.admin().getMetaRole(), role.sup());
    }

    @Property
    public void whenCallingPutRole_CreateARoleWithDefaultProperties(
            @Open GraknGraph graph, @Unused Label label) {
        Role role = graph.putRole(label);

        Assert.assertThat("The role should be played by no types", role.playedByTypes().collect(toSet()), Matchers.empty());
        Assert.assertThat("The role should be owned by no relation types", role.relationTypes().collect(toSet()), Matchers.empty());
    }

    @Property
    public void whenCallingPutRoleWithAnExistingRoleLabel_ItReturnsThatRole(
            @Open GraknGraph graph, @FromGraph Role role) {
        Role newType = graph.putRole(role.getLabel());
        Assert.assertEquals(role, newType);
    }

    @Property
    public void whenCallingPutRoleWithAnExistingTypeLabel_Throw(
            @Open GraknGraph graph, @FromGraph Type type) {
        exception.expect(GraphOperationException.class);
        if(Schema.MetaSchema.isMetaLabel(type.getLabel())){
            exception.expectMessage(ErrorMessage.RESERVED_WORD.getMessage(type.getLabel().getValue()));
        } else {
            exception.expectMessage(PropertyNotUniqueException.cannotCreateProperty(type, Schema.VertexProperty.ONTOLOGY_LABEL, type.getLabel()).getMessage());
        }
        graph.putRole(type.getLabel());
    }
}
