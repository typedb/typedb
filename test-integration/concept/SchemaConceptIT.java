/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.impl.AttributeTypeImpl;
import grakn.core.concept.impl.EntityTypeImpl;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.rule.GraknTestServer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SchemaConceptIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenChangingSchemaConceptLabel_EnsureLabelIsChangedAndOldLabelIsDead(){
        Label originalLabel = Label.of("my original label");
        Label newLabel = Label.of("my new label");
        EntityType entityType = tx.putEntityType(originalLabel.getValue());

        //Original label works
        assertEquals(entityType, tx.getType(originalLabel));

        //Change The Label
        entityType.label(newLabel);

        //Check old label is dead
        assertNull(tx.getType(originalLabel));

        // check new label is not dead
        assertNotNull(tx.getSchemaConcept(newLabel));
        assertEquals(entityType, tx.getType(newLabel));

        //Check the label is changes
        assertEquals(newLabel, entityType.label());

    }

    @Test
    public void whenSpecifyingTheResourceTypeOfAnEntityType_EnsureTheImplicitStructureIsCreated(){
        Label resourceLabel = Label.of("Attribute Type");
        EntityType entityType = tx.putEntityType("Entity1");
        AttributeType attributeType = tx.putAttributeType("Attribute Type", AttributeType.DataType.STRING);

        //Implicit Names
        Label hasResourceOwnerLabel = Schema.ImplicitType.HAS_OWNER.getLabel(resourceLabel);
        Label hasResourceValueLabel = Schema.ImplicitType.HAS_VALUE.getLabel(resourceLabel);
        Label hasResourceLabel = Schema.ImplicitType.HAS.getLabel(resourceLabel);

        entityType.has(attributeType);

        RelationType relationType = tx.getRelationType(hasResourceLabel.getValue());
        Assert.assertEquals(hasResourceLabel, relationType.label());

        Set<Label> roleLabels = relationType.roles().map(SchemaConcept::label).collect(toSet());
        assertThat(roleLabels, containsInAnyOrder(hasResourceOwnerLabel, hasResourceValueLabel));

        assertThat(entityType.playing().collect(toSet()), containsInAnyOrder(tx.getRole(hasResourceOwnerLabel.getValue())));
        assertThat(attributeType.playing().collect(toSet()), containsInAnyOrder(tx.getRole(hasResourceValueLabel.getValue())));

        //Check everything is implicit
        assertTrue(relationType.isImplicit());
        relationType.roles().forEach(role -> assertTrue(role.isImplicit()));

        // Check that resource is not required
        EdgeElement entityPlays = ((EntityTypeImpl) entityType).vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).iterator().next();
        assertFalse(entityPlays.propertyBoolean(Schema.EdgeProperty.REQUIRED));
        EdgeElement resourcePlays = ((AttributeTypeImpl<?>) attributeType).vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS).iterator().next();
        assertFalse(resourcePlays.propertyBoolean(Schema.EdgeProperty.REQUIRED));
    }

    @Test
    public void whenChangingTheLabelOfSchemaConceptAndThatLabelIsTakenByAnotherConcept_Throw(){
        Label label = Label.of("mylabel");

        EntityType e1 = tx.putEntityType("Entity1");
        tx.putEntityType(label);

        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(ErrorMessage.LABEL_TAKEN.getMessage(label));

        e1.label(label);
    }
}