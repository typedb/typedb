/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.test.property.kb;

import ai.grakn.GraknTx;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.generator.AbstractSchemaConceptGenerator.Meta;
import ai.grakn.generator.AbstractSchemaConceptGenerator.NonMeta;
import ai.grakn.generator.AbstractTypeGenerator.NonAbstract;
import ai.grakn.generator.FromTxGenerator.FromTx;
import ai.grakn.generator.GraknTxs.Open;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static java.util.stream.Collectors.toSet;
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
            @Open GraknTx graph, @FromTx @NonMeta EntityType type) {
        assumeThat(type.instances().collect(toSet()), empty());
        assumeThat(type.subs().collect(toSet()), contains(type));
        assumeThat(type.whenRules().collect(toSet()), empty());
        assumeThat(type.thenRules().collect(toSet()), empty());

        type.delete();

        assertNull(graph.getSchemaConcept(type.label()));
    }

    @Property
    public void whenAddingAnEntityOfTheMetaEntityType_Throw(@Meta EntityType type) {
        exception.expect(GraknTxOperationException.class);
        exception.expectMessage(GraknTxOperationException.metaTypeImmutable(type.label()).getMessage());
        type.create();
    }

    @Property
    public void whenAddingAnEntity_TheDirectTypeOfTheEntityIsTheTypeItWasCreatedFrom(
            @NonMeta @NonAbstract EntityType type) {
        Entity entity = type.create();

        assertEquals(type, entity.type());
    }

    @Property
    public void whenAddingAnEntity_TheEntityIsInNoRelations(@NonMeta @NonAbstract EntityType type) {
        Entity entity = type.create();

        assertThat(entity.relationships().collect(toSet()), empty());
    }

    @Property
    public void whenAddingAnEntity_TheEntityHasNoResources(@NonMeta @NonAbstract EntityType type) {
        Entity entity = type.create();

        assertThat(entity.attributes().collect(toSet()), empty());
    }
}