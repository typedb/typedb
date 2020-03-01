/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.keyspace;

import grakn.core.common.config.ConfigKey;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.TransactionImpl;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ShardManagerIT {
    private Session session;

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() {
        session.close();
    }

    @Test
    public void whenTxsCreateShardsForDifferentTypes_weDontLock() throws ExecutionException, InterruptedException {
        int threads = 8;
        long shardThreshold = 10L;
        server.serverConfig().setConfigProperty(ConfigKey.TYPE_SHARD_THRESHOLD, shardThreshold);
        try(Transaction tx = session.writeTransaction()){
            for (int threadNo = 0; threadNo < threads; threadNo++) {
                tx.putEntityType("someEntity" + threadNo);
            }
            tx.commit();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = Graql.parse("insert $x isa someEntity" + threadNo + ";").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                for(int q = 0 ; q < shardThreshold; q++) tx.execute(query);
                tx.computeShardCandidates();
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertFalse(tx.commitLockRequired());
                tx.commit();
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }
        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();

        assertFalse(session.shardManager().lockCandidatesPresent());
        assertFalse(session.shardManager().shardRequestsPresent());
    }

    @Test
    public void whenMultipleTxsCreateShardsForTheSameType_weLock() throws ExecutionException, InterruptedException {
        int threads = 8;
        long shardThreshold = 10L;
        server.serverConfig().setConfigProperty(ConfigKey.TYPE_SHARD_THRESHOLD, shardThreshold);
        try(Transaction tx = session.writeTransaction()){
            tx.putEntityType("someEntity");
            tx.commit();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = Graql.parse("insert $x isa someEntity;").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                for(int q = 0 ; q < shardThreshold; q++) tx.execute(query);
                tx.computeShardCandidates();
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertTrue(tx.commitLockRequired());
                tx.commit();
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }
        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();

        assertFalse(session.shardManager().lockCandidatesPresent());
        assertFalse(session.shardManager().shardRequestsPresent());
    }
}
