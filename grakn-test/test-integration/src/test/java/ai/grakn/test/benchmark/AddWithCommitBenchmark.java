/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.benchmark;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.test.rule.SessionContext;
import org.junit.Rule;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;


@State(Scope.Benchmark)
public class AddWithCommitBenchmark extends BenchmarkTest {

    @Rule
    public final SessionContext sessionContext = SessionContext.create();

    private GraknSession session;
    private EntityType entityType;
    private RelationshipType relationshipType;
    private Role role1;
    private Role role2;

    @Setup
    public void setup() throws Throwable {
        session = sessionContext.newSession();
        try(GraknTx tx = session.transaction(GraknTxType.WRITE)) {
            role1 = tx.putRole("benchmark_role1");
            role2 = tx.putRole("benchmark_role2");
            entityType = tx.putEntityType("benchmark_Entitytype").play(role1).play(role2);
            relationshipType = tx.putRelationshipType("benchmark_relationshipType").relate(role1).relate(role2);
            tx.commit();
        }
    }

    @Benchmark
    public void addEntity() {
        try(GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            entityType.create();
            graph.commit();
        }
    }

    @Benchmark
    public void addRelation() {
        try(GraknTx graph = session.transaction(GraknTxType.WRITE)) {
            Entity entity1 = entityType.create();
            Entity entity2 = entityType.create();
            relationshipType.create().assign(role1, entity1).assign(role2, entity2);
            graph.commit();
        }
    }
}
