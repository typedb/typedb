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
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static graql.lang.Graql.define;
import static graql.lang.Graql.insert;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;

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

//    @Test
//    public void a() {
//        try (TransactionOLTP tx1 = session.transaction().write()) {
//            tx1.execute(Graql.define(Graql.type("person").sub("entity")));
//            tx1.commit();
//        }
//        try (TransactionOLTP tx1 = session.transaction().write()) {
//            tx1.execute(insert(var("p").isa("person")));
//            tx1.commit();
//        }
////        try (TransactionOLTP tx1 = session.transaction().write()) {
////            tx1.shard(tx1.getEntityType("person").id());
////            tx1.commit();
////        }
//        try (TransactionOLTP tx1 = session.transaction().write()) {
//            tx1.execute(insert(var("p").isa("person")));
//            tx1.commit();
//        }
////        try (TransactionOLTP tx1 = session.transaction().write()) {
////            tx1.shard(tx1.getEntityType("person").id());
////            tx1.commit();
////        }
//        try (TransactionOLTP tx1 = session.transaction().write()) {
//            tx1.execute(insert(var("p").isa("person")));
//            tx1.commit();
//        }
//    }

    static class TransactionOLTPIT {
        @ClassRule
        public static GraknTestServer graknTestServer = new GraknTestServer();
        public SessionImpl session;

        @Before
        public void before() {
            session = graknTestServer.sessionWithNewKeyspace();
        }

        @After
        public void after() {
            session.close();
        }

        @Test
        public void verifyThatTypeShardingIsPerformedOnAGivenTypeIfThresholdIsReached() {
            TransactionOLTP.TYPE_SHARD_THRESHOLD = 1;
            try (TransactionOLTP tx = session.transaction().write()) {
                tx.execute(define(type("person").sub("entity")).asDefine());
                tx.commit();
            }
            try (TransactionOLTP tx = session.transaction().write()) {
                tx.execute(insert(var("p").isa("person")).asInsert());
                tx.commit();
            }
            // TODO: verify that no new type shard
            try (TransactionOLTP tx = session.transaction().write()) {
                tx.execute(insert(var("p").isa("person")).asInsert());
                tx.commit();
            }
            // TODO: verify that a new type shard is created for the type 'person'
        }

        @Test
        public void verifyThatTypeShardingIsPerformedOnTheRightEntity() {
            TransactionOLTP.TYPE_SHARD_THRESHOLD = 1;
            try (TransactionOLTP tx = session.transaction().write()) {
                tx.execute(define(type("person").sub("entity")).asDefine());
                tx.execute(define(type("company").sub("entity")).asDefine());
                tx.commit();
            }

            // insert two people and a company
            try (TransactionOLTP tx = session.transaction().write()) {
                tx.execute(insert(var("p").isa("person")).asInsert());
                tx.execute(insert(var("p").isa("person")).asInsert());
                tx.execute(insert(var("c").isa("company")).asInsert());
                tx.commit();
            }
            // TODO: verify that 'person' is sharded
            // TODO: verify that 'company' is not sharded
        }
    }

    static class TypeShardTest {
        @Test
        public void verifyThatShardAlgorithmWorks() {
            // shard returns void. how should I test it? how should I change the interface in order to make it testable?
        }
    }
}
