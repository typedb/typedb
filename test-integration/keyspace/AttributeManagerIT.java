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

import grakn.core.kb.concept.api.AttributeType;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AttributeManagerIT {
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
    public void whenTxsInsertDifferentAttributes_weDontLock() throws ExecutionException, InterruptedException {
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").has(someAttribute);
            tx.commit();
        }
        int threads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = Graql.parse("insert $x " + threadNo + " isa someAttribute;").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }

    @Test
    public void whenTxsInsertDifferentKeys_weDontLock() throws ExecutionException, InterruptedException {
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").key(someAttribute);
            tx.commit();
        }
        int threads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = Graql.parse("insert $x " + threadNo + " isa someAttribute;").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }

    @Test
    public void whenMultipleTxsInsertDifferentAttributesAsAKeyOrNot_weDontLock() throws ExecutionException, InterruptedException {
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").has(someAttribute);
            tx.putEntityType("keyEntity").key(someAttribute);
            tx.commit();
        }
        int threads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = threadNo % 2 == 0?
                    Graql.parse("insert $x isa someEntity, has someAttribute " + threadNo + ";").asInsert() :
                    Graql.parse("insert $x isa keyEntity, has someAttribute " + threadNo + ";").asInsert() ;
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }

    @Test
    public void whenMultipleTxsInsertExistingAttributes_weDontLock() throws ExecutionException, InterruptedException {
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").has(someAttribute);
            someAttribute.create(1337L);
            someAttribute.create(1667L);
            tx.commit();
        }
        int threads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = threadNo % 2 == 0?
                    Graql.parse("insert $x 1337 isa someAttribute;").asInsert() :
                    Graql.parse("insert $x 1667 isa someAttribute;").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }

    @Test
    public void whenMultipleTxsInsertSameAttribute_weLock() throws ExecutionException, InterruptedException {
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").has(someAttribute);
            tx.commit();
        }
        int threads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = Graql.parse("insert $x 1337 isa someAttribute;").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }

    @Test
    public void whenMultipleTxsInsertSameAttributeAsAKeyOrNot_weLock() throws ExecutionException, InterruptedException {
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").has(someAttribute);
            tx.putEntityType("keyEntity").key(someAttribute);
            tx.commit();
        }
        int threads = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            //lower half of the threads insert keys 0, 1 ,2 ...
            //uper half of the threads insert attributes 0, 1, 2, ...
            GraqlInsert query = threadNo < threads/2?
                    Graql.parse("insert $x isa keyEntity, has someAttribute " + threadNo + ";").asInsert() :
                    Graql.parse("insert $x isa someEntity, has someAttribute " + (threadNo - threads/2) + ";").asInsert() ;
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }

    @Test
    public void whenMultipleTxsAttachExistingAttributesAsKeys_weLock() throws ExecutionException, InterruptedException {
        int threads = 8;
        try(Transaction tx = session.writeTransaction()){
            AttributeType<Long> someAttribute = tx.putAttributeType("someAttribute", AttributeType.DataType.LONG);
            tx.putEntityType("someEntity").key(someAttribute);
            for (int threadNo = 0; threadNo < threads; threadNo++) {
                GraqlInsert query = Graql.parse("insert $x " + threadNo + " isa someAttribute;").asInsert();
                tx.execute(query);
            }
            tx.commit();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            GraqlInsert query = Graql.parse("insert $x isa someEntity, has someAttribute " + threadNo + ";").asInsert();
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                TransactionImpl tx = (TransactionImpl) session.writeTransaction();
                tx.execute(query);
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

        assertFalse(session.attributeManager().lockCandidatesPresent());
        assertFalse(session.attributeManager().ephemeralAttributesPresent());
    }
}
