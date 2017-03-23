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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;

import java.util.function.Consumer;

import static ai.grakn.graql.Graql.and;

public class AbstractGraph extends TestGraph {

    private static ResourceType<String> key;
    private static EntityType P, Q, p, q, r, s, t, u;
    private static RelationType rel, REL;
    private static RoleType relRoleA, relRoleB, RELRoleA, RELRoleB;

    private static Instance instanceU, instanceT, instanceP;

    public static Consumer<GraknGraph> get() {
        return new AbstractGraph().build();
    }

    @Override
    protected void buildOntology(GraknGraph graph) {
        key = graph.putResourceType("name", ResourceType.DataType.STRING);

        relRoleA = graph.putRoleType("rel-roleA");
        relRoleB = graph.putRoleType("rel-roleB");
        rel = graph.putRelationType("rel").hasRole(relRoleA).hasRole(relRoleB);

        RELRoleA = graph.putRoleType("REL-roleA");
        RELRoleB = graph.putRoleType("REL-roleB");
        REL = graph.putRelationType("REL").hasRole(RELRoleA).hasRole(RELRoleB);

        P = graph.putEntityType("P").playsRole(RELRoleA).playsRole(RELRoleB);
        P.hasResource(key);
        Q = graph.putEntityType("Q").playsRole(RELRoleB).playsRole(RELRoleA);
        p = graph.putEntityType("p").playsRole(relRoleA).playsRole(RELRoleA);
        q = graph.putEntityType("q").playsRole(relRoleB).playsRole(RELRoleB);
        r = graph.putEntityType("r").playsRole(relRoleA).playsRole(RELRoleA);
        s = graph.putEntityType("s").playsRole(relRoleB).playsRole(RELRoleB);
        u = graph.putEntityType("u").playsRole(relRoleA).playsRole(RELRoleA);
        u.hasResource(key);
        t = graph.putEntityType("t").playsRole(relRoleB).playsRole(RELRoleB);
        t.hasResource(key);
    }

    @Override
    protected void buildInstances(GraknGraph graph) {
        instanceU = putEntity(graph, "instanceU", u, key.getName());
        instanceT = putEntity(graph, "instanceT", t, key.getName());
        instanceP = putEntity(graph, "instanceP", P, key.getName());
    }

    @Override
    protected void  buildRelations(GraknGraph graph) {
        rel.addRelation()
                .putRolePlayer(relRoleA, instanceU)
                .putRolePlayer(relRoleB, instanceT);
        REL.addRelation()
                .putRolePlayer(RELRoleA, instanceU)
                .putRolePlayer(RELRoleB, instanceP);

    }
    @Override
    protected void buildRules(GraknGraph graph) {
        RuleType inferenceRule = graph.admin().getMetaRuleInference();

        Pattern R1_LHS = and(graph.graql().parsePatterns("$x isa p;$y isa q;($x, $y) isa rel;"));
        Pattern R1_RHS = and(graph.graql().parsePatterns("$x isa Q;"));
        inferenceRule.putRule(R1_LHS, R1_RHS);

        Pattern R2_LHS = and(graph.graql().parsePatterns("$x isa r;"));
        Pattern R2_RHS = and(graph.graql().parsePatterns("$x isa p;"));
        inferenceRule.putRule(R2_LHS, R2_RHS);

        Pattern R3_LHS = and(graph.graql().parsePatterns("$x isa s;"));
        Pattern R3_RHS = and(graph.graql().parsePatterns("$x isa p;"));
        inferenceRule.putRule(R3_LHS, R3_RHS);

        Pattern R4_LHS = and(graph.graql().parsePatterns("$x isa t;"));
        Pattern R4_RHS = and(graph.graql().parsePatterns("$x isa q;"));
        inferenceRule.putRule(R4_LHS, R4_RHS);

        Pattern R5_LHS = and(graph.graql().parsePatterns("$x isa u;"));
        Pattern R5_RHS = and(graph.graql().parsePatterns("$x isa r;"));
        inferenceRule.putRule(R5_LHS, R5_RHS);
    }
}
