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

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.rule.SampleKBContext;
import java.util.function.Consumer;

/**
 * Defines a scalability test defined in terms of number of rules in the system. Creates a simple rule chain:
 *
 * R_i(x, y) := R_{i-1}(x, y);     i e [1, N]
 *
 * with a single initial relation instance R_0(a ,b).
 *
 * @author Kasper Piskorski
 *
 */
public class SimpleRuleChainKB extends TestKB {
    private final int N;

    private SimpleRuleChainKB(int N){
        this.N = N;
    }

    public static SampleKBContext context(int N) {
        return new SimpleRuleChainKB(N).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            buildDB(graph, N);
        };
    }

    private void buildDB(GraknTx tx, int N) {
        Role fromRole = tx.putRole("fromRole");
        Role toRole = tx.putRole("toRole");

        RelationshipType relation0 = tx.putRelationshipType("relation0")
                .relates(fromRole)
                .relates(toRole);

        for (int i = 1; i <= N; i++) {
            tx.putRelationshipType("relation" + i)
                    .relates(fromRole)
                    .relates(toRole);
        }
        EntityType genericEntity = tx.putEntityType("genericEntity")
                .plays(fromRole)
                .plays(toRole);

        Entity fromEntity = genericEntity.addEntity();
        Entity toEntity = genericEntity.addEntity();

        relation0.addRelationship()
                .addRolePlayer(fromRole, fromEntity)
                .addRolePlayer(toRole, toEntity);

        for (int i = 1; i <= N; i++) {
            Var fromVar = Graql.var().asUserDefined();
            Var toVar = Graql.var().asUserDefined();
            VarPattern rulePattern = Graql
                    .label("rule" + i)
                    .sub("rule")
                    .when(
                            Graql.and(
                                    Graql.var()
                                            .rel(Graql.label(fromRole.getLabel()), fromVar)
                                            .rel(Graql.label(toRole.getLabel()), toVar)
                                            .isa("relation" + (i - 1))
                            )
                    )
                    .then(
                            Graql.and(
                                    Graql.var()
                                            .rel(Graql.label(fromRole.getLabel()), fromVar)
                                            .rel(Graql.label(toRole.getLabel()), toVar)
                                            .isa("relation" + i)
                            )
                    );
            tx.graql().define(rulePattern).execute();
        }
    }
}
