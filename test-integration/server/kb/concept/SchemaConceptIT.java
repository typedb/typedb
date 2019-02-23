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

package grakn.core.server.kb.concept;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SchemaConceptIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
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

        //Check the label is changes
        assertEquals(newLabel, entityType.label());
        assertEquals(entityType, tx.getType(newLabel));

        //Check old label is dead
        assertNull(tx.getType(originalLabel));
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

        RelationType relationshipType = tx.getRelationType(hasResourceLabel.getValue());
        Assert.assertEquals(hasResourceLabel, relationshipType.label());

        Set<Label> roleLabels = relationshipType.roles().map(SchemaConcept::label).collect(toSet());
        assertThat(roleLabels, containsInAnyOrder(hasResourceOwnerLabel, hasResourceValueLabel));

        assertThat(entityType.playing().collect(toSet()), containsInAnyOrder(tx.getRole(hasResourceOwnerLabel.getValue())));
        assertThat(attributeType.playing().collect(toSet()), containsInAnyOrder(tx.getRole(hasResourceValueLabel.getValue())));

        //Check everything is implicit
        assertTrue(relationshipType.isImplicit());
        relationshipType.roles().forEach(role -> assertTrue(role.isImplicit()));

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