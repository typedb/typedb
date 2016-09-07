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

package io.mindmaps.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import io.mindmaps.factory.MindmapsTestGraphFactory;

public class AbstractGraph {

    private static MindmapsGraph mindmaps;

    private static EntityType P, Q, p, q, r, s, t, u;
    private static RelationType rel, REL;
    private static RoleType relRoleA, relRoleB, RELRoleA, RELRoleB;

    private static Instance instanceU, instanceT, instanceP;

    public static MindmapsGraph getGraph() {
        mindmaps = MindmapsTestGraphFactory.newEmptyGraph();
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

        P = mindmaps.putEntityType("P").playsRole(RELRoleA).playsRole(RELRoleB);
        Q = mindmaps.putEntityType("Q").playsRole(RELRoleB).playsRole(RELRoleA);
        p = mindmaps.putEntityType("p").playsRole(relRoleA).playsRole(RELRoleA);
        q = mindmaps.putEntityType("q").playsRole(relRoleB).playsRole(RELRoleB);
        r = mindmaps.putEntityType("r").playsRole(relRoleA).playsRole(RELRoleA);
        s = mindmaps.putEntityType("s").playsRole(relRoleB).playsRole(RELRoleB);
        u = mindmaps.putEntityType("u").playsRole(relRoleA).playsRole(RELRoleA);
        t = mindmaps.putEntityType("t").playsRole(relRoleB).playsRole(RELRoleB);
    }

    private static void buildInstances() {
        instanceU = mindmaps.putEntity("instanceU", u);
        instanceT = mindmaps.putEntity("instanceT", t);
        instanceP = mindmaps.putEntity("instanceP", P);
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

        String R1_LHS = "match " +
                        "$x isa p;\n" +
                        "$y isa q;\n" +
                        "($x, $y) isa rel select $x";
        String R1_RHS = "match $x isa Q";
        mindmaps.putRule("R1", R1_LHS, R1_RHS, inferenceRule);

        String R2_LHS = "match $x isa r;";
        String R2_RHS = "match $x isa p";
        mindmaps.putRule("R2", R2_LHS, R2_RHS, inferenceRule);

        String R3_LHS = "match $x isa s;";
        String R3_RHS = "match $x isa p";
        mindmaps.putRule("R3", R3_LHS, R3_RHS, inferenceRule);

        String R4_LHS = "match $x isa t;";
        String R4_RHS = "match $x isa q";
        mindmaps.putRule("R4", R4_LHS, R4_RHS, inferenceRule);

        String R5_LHS = "match $x isa u;";
        String R5_RHS = "match $x isa r";
        Rule r5 = mindmaps.putRule("R5", R5_LHS, R5_RHS, inferenceRule);
    }

}
