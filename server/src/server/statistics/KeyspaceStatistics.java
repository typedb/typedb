/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.statistics;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.concept.ConceptVertex;
import grakn.core.server.keyspace.KeyspaceImpl;
import grakn.core.server.session.TransactionOLTP;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;


/**
 * This class is bound per-keyspace, and shared between sessions on the same keyspace just like the JanusGraph object.
 * The general method of operation is as a cache, into which the statistics delta is merged on commit.
 * At this point we also write the statistics to JanusGraph, writing recorded values as vertex properties on the schema
 * concepts.
 *
 * On cache miss, we read from JanusGraph schema vertices, which only have the INSTANCE_COUNT property if the
 * count is non-zero or has been non-zero in the past. No such property means instance count is 0.
 *
 * We also store the total count of all concepts the same was as any other schema concept, but on the `Thing` meta
 * concept. Note that this is different from the other instance counts as it DOES include counts of all subtypes. The
 * other counts on user-defined schema concepts are for for that concrete type only
 */
public class KeyspaceStatistics {
    private Cluster cluster;
    private KeyspaceImpl keyspace;

    public KeyspaceStatistics(Cluster cluster, KeyspaceImpl keyspace) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        try (Session connection = this.cluster.connect()) {
            connection.execute("create table if not exists " + keyspace.name() + ".statistics(label text, count counter, primary key(label));");
        }
    }

    public long count(TransactionOLTP tx, Label label) {
        try (Session connection = cluster.connect()) {
            List<Row> select = connection.execute("select * from " + keyspace.name() + ".statistics where label = '" + label.getValue() + "';").all();
            if (select.size() == 0) {
                if ((tx.getSchemaConcept(label) != null)) {
                    return 0L;
                }
                else {
                    return -1L;
                }
            } else if (select.size() == 1) {
                return select.get(0).getLong("count");
            } else {
                throw new RuntimeException("Keyspace statistics corrupted. There should only be one instance of  " + label.getValue() + " instances of " + select.size() + ". where there should only be one");
            }
        }
    }

    public void commit(TransactionOLTP tx, UncomittedStatisticsDelta statisticsDelta) {
        statisticsDelta.updateThingCount();
        try (Session connection = cluster.connect()) {
            statisticsDelta.instanceDeltas().forEach((label, count) -> {
                connection.execute("update " + keyspace.name() + ".statistics set count = count + " + count + " where label = '" + label.getValue() + "';");
            });
        }
    }
}