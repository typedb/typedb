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

package grakn.core.server.kb;

import grakn.client.GraknClient;
import graql.lang.Graql;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: remove
public class GskIT {
    public static final Logger LOG = LoggerFactory.getLogger(GskIT.class);

    @Test
    public void limitQuery() {
//        LOG.info("hi");
//        try (GraknClient graknClient = new GraknClient("localhost:48555")) {
//            try (GraknClient.Session session = graknClient.session("ten_milion")) {
//                try (GraknClient.Transaction tx = session.transaction().read()) {
//                    long start = System.currentTimeMillis();
//                    tx.execute(Graql.parse("match $s isa sentence; get; limit 1;").asGet());
//                    long elapsed = System.currentTimeMillis() - start;
//                    System.out.println("elapsed = " + elapsed + "ms");
//                }
//            }
//        }
    }

    static class TransactionOLTPIT {
//        @Test
        public void verifyThatShardIsPerformedOnAGivenTypeOnCommitIfThresholdIsReached() {
            // verify that 'person' is sharded
            // verify that 'company' is not sharded
        }

//        @Test
        public void verifyThatShardIsNotPerformedOnAGiveTypeIfThresholdIsNotReached() {
            // verify that 'person' is not sharded
            // verify that 'company' is not sharded
        }
    }

    static class TypeSharderTest {
//        @Test
        public void verifyThatShardAlgorithmWorks() {
            // shard returns void. how should I test it? how should I change the interface in order to make it testable?
        }
    }
}
