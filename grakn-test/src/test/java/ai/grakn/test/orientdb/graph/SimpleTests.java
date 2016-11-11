/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

package ai.grakn.test.orientdb.graph;

import ai.grakn.concept.Entity;
import ai.grakn.concept.RelationType;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.test.AbstractRollbackGraphTest;
import ai.grakn.Mindmaps;
import ai.grakn.MindmapsGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.exception.MindmapsValidationException;
import ai.grakn.test.AbstractRollbackGraphTest;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class SimpleTests extends AbstractRollbackGraphTest {

    @Ignore //Failing due to inconsistent clears.
    @Test
    public void testOrientDBConstructionThroughEngine() throws MindmapsValidationException {
        MindmapsGraph mindmapsGraph = Mindmaps.factory(Mindmaps.DEFAULT_URI, "memory").getGraph();

        //Create Ontology
        RoleType role1 = mindmapsGraph.putRoleType("role1");
        RoleType role2 = mindmapsGraph.putRoleType("role2");
        mindmapsGraph.putEntityType("et1").playsRole(role1);
        mindmapsGraph.putEntityType("et2").playsRole(role2);
        mindmapsGraph.putRelationType("rel").hasRole(role1).hasRole(role2);

        mindmapsGraph.commit();

        //Check Ontology is there:
        role1 = mindmapsGraph.getRoleType("role1");
        role2 = mindmapsGraph.getRoleType("role2");
        EntityType et1 = mindmapsGraph.getEntityType("et1").playsRole(role1);
        EntityType et2 = mindmapsGraph.getEntityType("et2").playsRole(role2);
        RelationType rel = mindmapsGraph.getRelationType("rel").hasRole(role1).hasRole(role2);

        assertNotNull(role1);
        assertNotNull(role2);
        assertNotNull(et1);
        assertNotNull(et2);
        assertNotNull(rel);

        //Create Some Data
        Entity e1 = mindmapsGraph.addEntity(et1);
        Entity e2 = mindmapsGraph.addEntity(et2);
        mindmapsGraph.addRelation(rel).putRolePlayer(role1, e1).putRolePlayer(role2, e2);

        mindmapsGraph.commit();

        //Check the Data is there
        mindmapsGraph = Mindmaps.factory(Mindmaps.DEFAULT_URI, "memory").getGraph();
        assertEquals(1, mindmapsGraph.getEntityType("et1").instances().size());
        assertEquals(1, mindmapsGraph.getEntityType("et2").instances().size());

        //Check Engine has Castings
        Cache cache = Cache.getInstance();
        assertEquals(2, cache.getCastingJobs(mindmapsGraph.getKeyspace()).size());
    }
}
