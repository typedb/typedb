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

package ai.grakn.graph.internal;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class OntologyConceptTest extends GraphTestBase {

    @Test
    public void whenChangingOntologyConceptLabel_EnsureLabelIsChangedAndOldLabelIsDead(){
        Label originalLabel = Label.of("my original label");
        Label newLabel = Label.of("my new label");
        EntityType entityType = graknGraph.putEntityType(originalLabel.getValue());

        //Original label works
        assertEquals(entityType, graknGraph.getType(originalLabel));

        //Change The Label
        entityType.setLabel(newLabel);

        //Check the label is changes
        assertEquals(newLabel, entityType.getLabel());
        assertEquals(entityType, graknGraph.getType(newLabel));

        //Check old label is dead
        assertNull(graknGraph.getType(originalLabel));
    }
}
