/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.reasoner.graphs;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;

public class AbstractGraph {

    private static MindmapsTransaction mindmaps;

    private static EntityType P, Q, p, q, r, s, t, u;
    private static RelationType rel, REL;
    private static RoleType relRoleA, relRoleB, RELRoleA, RELRoleB;

    private static Instance instanceU, instanceT, instanceP;

    private AbstractGraph() {
    }

    public static MindmapsTransaction getTransaction() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmaps = graph.newTransaction();
        buildGraph();

        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }

        return mindmaps;
    }

    private static void buildGraph() {
        buildOntology();
        buildInstances();
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {

        relRoleA = mindmaps.putRoleType("rel-roleA");
        relRoleB = mindmaps.putRoleType("rel-roleB");
        rel = mindmaps.putRelationType("rel").hasRole(relRoleA).hasRole(relRoleB);

        RELRoleA = mindmaps.putRoleType("REL-roleA");
        RELRoleB = mindmaps.putRoleType("REL-roleB");
        REL = mindmaps.putRelationType("REL").hasRole(RELRoleA).hasRole(RELRoleB);

        P = mindmaps.putEntityType("P").setValue("P").playsRole(RELRoleA).playsRole(RELRoleB);
        Q = mindmaps.putEntityType("Q").setValue("Q").playsRole(RELRoleB).playsRole(RELRoleA);
        p = mindmaps.putEntityType("p").setValue("p").playsRole(relRoleA).playsRole(RELRoleA);
        q = mindmaps.putEntityType("q").setValue("q").playsRole(relRoleB).playsRole(RELRoleB);
        r = mindmaps.putEntityType("r").setValue("r").playsRole(relRoleA).playsRole(RELRoleA);
        s = mindmaps.putEntityType("s").setValue("s").playsRole(relRoleB).playsRole(RELRoleB);
        u = mindmaps.putEntityType("u").setValue("u").playsRole(relRoleA).playsRole(RELRoleA);
        t = mindmaps.putEntityType("t").setValue("t").playsRole(relRoleB).playsRole(RELRoleB);

    }

    private static void buildInstances() {
        instanceU = putEntity(u, "instanceU");
        instanceT = putEntity(t, "instanceT");
        instanceP = putEntity(P, "instanceP");
    }

    private static void buildRelations() {
        mindmaps.addRelation(rel)
                .putRolePlayer(relRoleA, instanceU)
                .putRolePlayer(relRoleB, instanceT);
        mindmaps.addRelation(REL)
                .putRolePlayer(RELRoleA, instanceU)
                .putRolePlayer(RELRoleB, instanceP);

    }
    private static void buildRules() {

        RuleType inferenceRule = mindmaps.getMetaRuleInference();

        Rule r1 = mindmaps.putRule("R1", inferenceRule);
        Rule r2 = mindmaps.putRule("R2", inferenceRule);
        Rule r3 = mindmaps.putRule("R3", inferenceRule);
        Rule r4 = mindmaps.putRule("R4", inferenceRule);
        Rule r5 = mindmaps.putRule("R5", inferenceRule);

        String R1_LHS = "match " +
                        "$x isa p;\n" +
                        "$y isa q;\n" +
                        "($x, $y) isa rel select $x";
        String R1_RHS = "match $x isa Q";
        r1.setLHS(R1_LHS);
        r1.setRHS(R1_RHS);

        String R2_LHS = "match $x isa r;";
        String R2_RHS = "match $x isa p";
        r2.setLHS(R2_LHS);
        r2.setRHS(R2_RHS);

        String R3_LHS = "match $x isa s;";
        String R3_RHS = "match $x isa p";
        r3.setLHS(R3_LHS);
        r3.setRHS(R3_RHS);

        String R4_LHS = "match $x isa t;";
        String R4_RHS = "match $x isa q";
        r4.setLHS(R4_LHS);
        r4.setRHS(R4_RHS);

        String R5_LHS = "match $x isa u;";
        String R5_RHS = "match $x isa r";
        r5.setLHS(R5_LHS);
        r5.setRHS(R5_RHS);


    }


    private static Instance putEntity(EntityType type, String name) {
        return mindmaps.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type).setValue(name);
    }

}
