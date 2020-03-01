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
 */

package grakn.core.util;

import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlInsert;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import org.apache.commons.collections.CollectionUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Helper methods for writing tests for Graql
 *
 */
@SuppressWarnings("CheckReturnValue")
public class GraqlTestUtil {

    public static void assertExists(Transaction tx, Pattern... patterns) {
        assertTrue(tx.stream(Graql.match(patterns)).iterator().hasNext());
    }

    public static void assertExists(Transaction tx, MatchClause matchClause) {
        assertTrue(tx.stream(matchClause).iterator().hasNext());
    }

    public static void assertNotExists(Transaction tx, Pattern... patterns) {
        assertFalse(tx.stream(Graql.match(patterns)).iterator().hasNext());
    }

    public static void assertNotExists(Transaction tx, MatchClause matchClause) {
        assertFalse(tx.stream(matchClause).iterator().hasNext());
    }

    public static <T> void assertCollectionsEqual(Collection<T> c1, Collection<T> c2) {
        assertTrue("\nc1: " + c1 + "\n!=\nc2: " + c2 , CollectionUtils.isEqualCollection(c1, c2));
    }

    public static <T> void assertCollectionsEqual(String msg, Collection<T> c1, Collection<T> c2) {
        assertTrue(msg, CollectionUtils.isEqualCollection(c1, c2));
    }

    public static <T> void assertCollectionsNonTriviallyEqual(Collection<T> c1, Collection<T> c2){
        assertFalse("Trivial equality!", c1.isEmpty() && c2.isEmpty());
        assertCollectionsEqual(c1, c2);
    }

    public static <T> void assertCollectionsNonTriviallyEqual(String msg, Collection<T> c1, Collection<T> c2){
        assertFalse("Trivial equality!", c1.isEmpty() && c2.isEmpty());
        assertCollectionsEqual(msg, c1, c2);
    }

    public static void loadFromFile(String gqlPath, String file, Transaction tx) {
        try {
            System.out.println("Loading... " + gqlPath + file);
            InputStream inputStream = GraqlTestUtil.class.getClassLoader().getResourceAsStream(gqlPath + file);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Graql.parseList(s).forEach(tx::execute);
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    public static void loadFromFileAndCommit(String gqlPath, String file, Session session) {
        Transaction tx = session.writeTransaction();
        loadFromFile(gqlPath, file, tx);
        tx.commit();
    }


    public static Thing putEntityWithResource(Transaction tx, String id, EntityType type, Label key) {
        Thing inst = type.create();
        Attribute attributeInstance = tx.getAttributeType(key.getValue()).create(id);
        inst.has(attributeInstance);
        return inst;
    }

    public static Thing getInstance(Transaction tx, String id){
        Set<Thing> things = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(toSet());
        if (things.size() != 1) {
            throw new IllegalStateException("Multiple things with given resource value");
        }
        return things.iterator().next();
    }

    public static void insertStatementsConcurrently(Session session, List<Statement> statements,
                                                    int threads, int insertsPerCommit) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        int listSize = statements.size();
        int listChunk = listSize / threads;

        List<CompletableFuture<Void>> asyncInsertions = new ArrayList<>();
        for (int threadNo = 0; threadNo < threads; threadNo++) {
            boolean lastChunk = threadNo == threads - 1;
            final int startIndex = threadNo * listChunk;
            int endIndex = (threadNo + 1) * listChunk;
            if (endIndex > listSize && lastChunk) endIndex = listSize;

            List<Statement> subList = statements.subList(startIndex, endIndex);
            System.out.println("indices: " + startIndex + "-" + endIndex + " , size: " + subList.size());
            CompletableFuture<Void> asyncInsert = CompletableFuture.supplyAsync(() -> {
                long start2 = System.currentTimeMillis();
                Transaction tx = session.writeTransaction();
                int inserted = 0;
                for (Statement statement : subList) {
                    GraqlInsert insert = Graql.insert(statement);
                    tx.execute(insert);
                    inserted++;
                    if (inserted % insertsPerCommit == 0) {
                        tx.commit();
                        inserted = 0;
                        tx = session.writeTransaction();
                    }
                }
                if (inserted != 0) tx.commit();
                System.out.println("Thread: " + Thread.currentThread().getId() + " elements: " + subList.size() + " time: " + (System.currentTimeMillis() - start2));
                return null;
            }, executorService);
            asyncInsertions.add(asyncInsert);
        }
        CompletableFuture.allOf(asyncInsertions.toArray(new CompletableFuture[]{})).get();
        executorService.shutdown();
    }
}
