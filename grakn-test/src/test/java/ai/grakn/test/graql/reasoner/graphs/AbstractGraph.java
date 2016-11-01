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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;

import java.util.UUID;

public class AbstractGraph {

    private static GraknGraph grakn;

    private static EntityType P, Q, p, q, r, s, t, u;
    private static RelationType rel, REL;
    private static RoleType relRoleA, relRoleB, RELRoleA, RELRoleB;

    private static Instance instanceU, instanceT, instanceP;

    public static GraknGraph getGraph() {
        grakn = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        buildGraph();

        try {
            grakn.commit();
        } catch (GraknValidationException e) {
            System.out.println(e.getMessage());
        }

        return grakn;
    }

    private static void buildGraph() {
        buildOntology();
        buildInstances();
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {
        relRoleA = grakn.putRoleType("rel-roleA");
        relRoleB = grakn.putRoleType("rel-roleB");
        rel = grakn.putRelationType("rel").hasRole(relRoleA).hasRole(relRoleB);

        RELRoleA = grakn.putRoleType("REL-roleA");
        RELRoleB = grakn.putRoleType("REL-roleB");
        REL = grakn.putRelationType("REL").hasRole(RELRoleA).hasRole(RELRoleB);

        P = grakn.putEntityType("P").playsRole(RELRoleA).playsRole(RELRoleB);
        Q = grakn.putEntityType("Q").playsRole(RELRoleB).playsRole(RELRoleA);
        p = grakn.putEntityType("p").playsRole(relRoleA).playsRole(RELRoleA);
        q = grakn.putEntityType("q").playsRole(relRoleB).playsRole(RELRoleB);
        r = grakn.putEntityType("r").playsRole(relRoleA).playsRole(RELRoleA);
        s = grakn.putEntityType("s").playsRole(relRoleB).playsRole(RELRoleB);
        u = grakn.putEntityType("u").playsRole(relRoleA).playsRole(RELRoleA);
        t = grakn.putEntityType("t").playsRole(relRoleB).playsRole(RELRoleB);
    }

    private static void buildInstances() {
        instanceU = grakn.putEntity("instanceU", u);
        instanceT = grakn.putEntity("instanceT", t);
        instanceP = grakn.putEntity("instanceP", P);
    }

    private static void buildRelations() {
        grakn.addRelation(rel)
                .putRolePlayer(relRoleA, instanceU)
                .putRolePlayer(relRoleB, instanceT);
        grakn.addRelation(REL)
                .putRolePlayer(RELRoleA, instanceU)
                .putRolePlayer(RELRoleB, instanceP);

    }
    private static void buildRules() {
        RuleType inferenceRule = grakn.getMetaRuleInference();

        String R1_LHS = "$x isa p;$y isa q;($x, $y) isa rel;";
        String R1_RHS = "$x isa Q;";
        grakn.putRule("R1", R1_LHS, R1_RHS, inferenceRule);

        String R2_LHS = "$x isa r;";
        String R2_RHS = "$x isa p;";
        grakn.putRule("R2", R2_LHS, R2_RHS, inferenceRule);

        String R3_LHS = "$x isa s;";
        String R3_RHS = "$x isa p;";
        grakn.putRule("R3", R3_LHS, R3_RHS, inferenceRule);

        String R4_LHS = "$x isa t;";
        String R4_RHS = "$x isa q;";
        grakn.putRule("R4", R4_LHS, R4_RHS, inferenceRule);

        String R5_LHS = "$x isa u;";
        String R5_RHS = "$x isa r;";
        grakn.putRule("R5", R5_LHS, R5_RHS, inferenceRule);
    }

}
