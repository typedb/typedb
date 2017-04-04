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

package ai.grakn.graph.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.exception.ConceptException;
import ai.grakn.generator.AbstractTypeGenerator.Meta;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static ai.grakn.graph.property.PropertyUtil.choose;
import static ai.grakn.graph.property.PropertyUtil.directSubTypes;
import static ai.grakn.util.ErrorMessage.META_TYPE_IMMUTABLE;
import static ai.grakn.util.ErrorMessage.SUPER_TYPE_LOOP_DETECTED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 * @author Felix Chapman
 */
@RunWith(JUnitQuickcheck.class)
public class RelationTypePropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenDeletingMetaRelationType_Throw(@Open GraknGraph graph) {
        RelationType relation = graph.admin().getMetaRelationType();

        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(relation.getName()));

        relation.delete();
    }

    @Property
    public void whenANonMetaRelationTypeHasNoInstancesSubTypesOrRules_ItCanBeDeleted(
            @Open GraknGraph graph, @FromGraph @Meta(false) RelationType type) {
        assumeThat(type.instances(), empty());
        assumeThat(type.subTypes(), contains(type));
        assumeThat(type.getRulesOfHypothesis(), empty());
        assumeThat(type.getRulesOfConclusion(), empty());

        type.delete();

        assertNull(graph.getType(type.getName()));
    }

    @Property
    public void whenAddingARelationOfAMetaType_Throw(@Meta RelationType type) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(type.getName()));
        type.addRelation();
    }

    @Property
    public void whenAddingARelation_TheDirectTypeOfTheRelationIsTheTypeItWasCreatedFrom(
            @Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertEquals(type, relation.type());
    }

    @Property
    public void whenAddingARelation_TheRelationIsInNoRelations(@Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertThat(relation.relations(), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoResources(@Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertThat(relation.resources(), empty());
    }

    @Property
    public void whenAddingARelation_TheRelationHasNoRolePlayers(@Meta(false) RelationType type) {
        Relation relation = type.addRelation();

        assertThat(relation.rolePlayers(), empty());
    }

    @Property
    public void whenSettingTheDirectSuperTypeOfAMetaType_Throw(
            @Meta RelationType subType, @FromGraph RelationType superType) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subType.getName()));
        subType.superType(superType);
    }

    @Property
    public void whenSettingTheDirectSuperTypeToAnIndirectSubType_Throw(
            @Meta(false) RelationType type, long seed) {
        RelationType newSuperType = choose(type.subTypes(), seed);

        exception.expect(ConceptException.class);
        exception.expectMessage(SUPER_TYPE_LOOP_DETECTED.getMessage(type.getName(), newSuperType.getName()));
        type.superType(newSuperType);
    }

    @Property
    public void whenSettingTheDirectSuperType_TheDirectSuperTypeIsSet(
            @Meta(false) RelationType subType, @FromGraph RelationType superType) {
        assumeThat(subType.subTypes(), not(hasItem(superType)));

        subType.superType(superType);

        assertEquals(superType, subType.superType());
    }

    @Property
    public void whenAddingADirectSubTypeThatIsAMetaType_Throw(
            RelationType superType, @Meta @FromGraph RelationType subType) {
        exception.expect(ConceptException.class);
        exception.expectMessage(META_TYPE_IMMUTABLE.getMessage(subType.getName()));
        superType.subType(subType);
    }

    @Property
    public void whenAddingADirectSubTypeWhichIsAnIndirectSuperType_Throw(
            @Meta(false) RelationType newSubType, long seed) {
        RelationType type = choose(newSubType.subTypes(), seed);

        exception.expect(ConceptException.class);
        exception.expectMessage(SUPER_TYPE_LOOP_DETECTED.getMessage(newSubType.getName(), type.getName()));
        type.subType(newSubType);
    }

    @Property
    public void whenAddingADirectSubType_TheDirectSubTypeIsAdded(
            @Open GraknGraph graph, @FromGraph RelationType superType, @Meta(false) @FromGraph RelationType subType) {
        assumeThat(subType.subTypes(), not(hasItem(superType)));

        superType.subType(subType);

        assertThat(directSubTypes(graph, superType), hasItem(subType));
    }
}
