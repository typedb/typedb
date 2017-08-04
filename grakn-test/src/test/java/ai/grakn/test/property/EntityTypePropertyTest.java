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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.generator.AbstractOntologyConceptGenerator.Meta;
import ai.grakn.generator.AbstractOntologyConceptGenerator.NonMeta;
import ai.grakn.generator.AbstractTypeGenerator.NonAbstract;
import ai.grakn.generator.FromGraphGenerator.FromGraph;
import ai.grakn.generator.GraknGraphs.Open;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/**
 * @author Felix Chapman
 */
@RunWith(JUnitQuickcheck.class)
public class EntityTypePropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void whenANonMetaEntityTypeHasNoInstancesSubTypesOrRules_ItCanBeDeleted(
            @Open GraknGraph graph, @FromGraph @NonMeta EntityType type) {
        assumeThat(type.instances(), empty());
        assumeThat(type.subs(), contains(type));
        assumeThat(type.getRulesOfHypothesis(), empty());
        assumeThat(type.getRulesOfConclusion(), empty());

        type.delete();

        assertNull(graph.getOntologyConcept(type.getLabel()));
    }

    @Property
    public void whenAddingAnEntityOfTheMetaEntityType_Throw(@Meta EntityType type) {
        exception.expect(GraphOperationException.class);
        exception.expectMessage(GraphOperationException.metaTypeImmutable(type.getLabel()).getMessage());
        type.addEntity();
    }

    @Property
    public void whenAddingAnEntity_TheDirectTypeOfTheEntityIsTheTypeItWasCreatedFrom(
            @NonMeta @NonAbstract EntityType type) {
        Entity entity = type.addEntity();

        assertEquals(type, entity.type());
    }

    @Property
    public void whenAddingAnEntity_TheEntityIsInNoRelations(@NonMeta @NonAbstract EntityType type) {
        Entity entity = type.addEntity();

        assertThat(entity.relations(), empty());
    }

    @Property
    public void whenAddingAnEntity_TheEntityHasNoResources(@NonMeta @NonAbstract EntityType type) {
        Entity entity = type.addEntity();

        assertThat(entity.resources(), empty());
    }
}
