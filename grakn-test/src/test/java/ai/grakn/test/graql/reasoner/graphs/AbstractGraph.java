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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.graql.Pattern;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;

import static ai.grakn.graql.Graql.and;

public class AbstractGraph extends TestGraph{

    private static EntityType P, Q, p, q, r, s, t, u;
    private static RelationType rel, REL;
    private static RoleType relRoleA, relRoleB, RELRoleA, RELRoleB;

    private static Instance instanceU, instanceT, instanceP;

    public static GraknGraph getGraph() {
        return new AbstractGraph().graph();
    }

    @Override
    protected void buildOntology() {
        relRoleA = graknGraph.putRoleType("rel-roleA");
        relRoleB = graknGraph.putRoleType("rel-roleB");
        rel = graknGraph.putRelationType("rel").hasRole(relRoleA).hasRole(relRoleB);

        RELRoleA = graknGraph.putRoleType("REL-roleA");
        RELRoleB = graknGraph.putRoleType("REL-roleB");
        REL = graknGraph.putRelationType("REL").hasRole(RELRoleA).hasRole(RELRoleB);

        P = graknGraph.putEntityType("P").playsRole(RELRoleA).playsRole(RELRoleB);
        Q = graknGraph.putEntityType("Q").playsRole(RELRoleB).playsRole(RELRoleA);
        p = graknGraph.putEntityType("p").playsRole(relRoleA).playsRole(RELRoleA);
        q = graknGraph.putEntityType("q").playsRole(relRoleB).playsRole(RELRoleB);
        r = graknGraph.putEntityType("r").playsRole(relRoleA).playsRole(RELRoleA);
        s = graknGraph.putEntityType("s").playsRole(relRoleB).playsRole(RELRoleB);
        u = graknGraph.putEntityType("u").playsRole(relRoleA).playsRole(RELRoleA);
        t = graknGraph.putEntityType("t").playsRole(relRoleB).playsRole(RELRoleB);
    }

    @Override
    protected void buildInstances() {
        instanceU = putEntity("instanceU", u);
        instanceT = putEntity("instanceT", t);
        instanceP = putEntity("instanceP", P);
    }

    @Override
    protected void  buildRelations() {
        rel.addRelation()
                .putRolePlayer(relRoleA, instanceU)
                .putRolePlayer(relRoleB, instanceT);
        REL.addRelation()
                .putRolePlayer(RELRoleA, instanceU)
                .putRolePlayer(RELRoleB, instanceP);

    }
    @Override
    protected void buildRules() {
        RuleType inferenceRule = graknGraph.getMetaRuleInference();

        Pattern R1_LHS = and(graknGraph.graql().parsePatterns("$x isa p;$y isa q;($x, $y) isa rel;"));
        Pattern R1_RHS = and(graknGraph.graql().parsePatterns("$x isa Q;"));
        inferenceRule.addRule(R1_LHS, R1_RHS);

        Pattern R2_LHS = and(graknGraph.graql().parsePatterns("$x isa r;"));
        Pattern R2_RHS = and(graknGraph.graql().parsePatterns("$x isa p;"));
        inferenceRule.addRule(R2_LHS, R2_RHS);

        Pattern R3_LHS = and(graknGraph.graql().parsePatterns("$x isa s;"));
        Pattern R3_RHS = and(graknGraph.graql().parsePatterns("$x isa p;"));
        inferenceRule.addRule(R3_LHS, R3_RHS);

        Pattern R4_LHS = and(graknGraph.graql().parsePatterns("$x isa t;"));
        Pattern R4_RHS = and(graknGraph.graql().parsePatterns("$x isa q;"));
        inferenceRule.addRule(R4_LHS, R4_RHS);

        Pattern R5_LHS = and(graknGraph.graql().parsePatterns("$x isa u;"));
        Pattern R5_RHS = and(graknGraph.graql().parsePatterns("$x isa r;"));
        inferenceRule.addRule(R5_LHS, R5_RHS);
    }
}
