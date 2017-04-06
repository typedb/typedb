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

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.ConceptException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static ai.grakn.util.ErrorMessage.ID_ALREADY_TAKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RelationTypeTest extends GraphTestBase{
    private RelationType relationType;
    private RoleType role1;
    private RoleType role2;
    private RoleType role3;

    @Before
    public void setUp() throws ConceptException {
        //Building
        relationType = graknGraph.putRelationType("relationTypes");
        role1 = graknGraph.putRoleType("role1");
        role2 = graknGraph.putRoleType("role2");
        role3 = graknGraph.putRoleType("role3");

        relationType.relates(role1);
        relationType.relates(role2);
        relationType.relates(role3);
    }

    @Test
    public void updateBaseTypeCheck(){
        RelationType relationType = graknGraph.putRelationType("Test");
        RelationType relationType2 = graknGraph.putRelationType("Test");
        assertEquals(relationType, relationType2);
    }

    @Test
    public void overrideFail(){
        RoleType original = graknGraph.putRoleType("Role Type");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(ID_ALREADY_TAKEN.getMessage(original.getLabel(), original.toString()));

        graknGraph.putRelationType(original.getLabel());
    }

    @Test
    public void testGetRoles() throws Exception {
        Collection<RoleType> roles = relationType.relates();
        assertEquals(3, roles.size());
        assertTrue(roles.contains(role1));
        assertTrue(roles.contains(role2));
        assertTrue(roles.contains(role3));
    }

    @Test
    public void testRoleType(){
        RelationType c1 = graknGraph.putRelationType("c1");
        RoleType c2 = graknGraph.putRoleType("c2");
        RoleType c3 = graknGraph.putRoleType("c3");
        assertTrue(c2.relationTypes().isEmpty());

        c1.relates(c2);
        c1.relates(c3);
        assertTrue(c1.relates().contains(c2));
        assertTrue(c1.relates().contains(c3));

        c1.deleteRelates(c2);
        assertFalse(c1.relates().contains(c2));
        assertTrue(c1.relates().contains(c3));
    }


}