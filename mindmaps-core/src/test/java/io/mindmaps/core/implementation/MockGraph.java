/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class MockGraph implements Graph {
    @Override
    public Vertex addVertex(Object... objects) {
        return null;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return null;
    }

    @Override
    public Transaction tx() {
        return new MockTransaction();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    public class MockTransaction implements Transaction{

        @Override
        public void open() {

        }

        @Override
        public void commit() {

        }

        @Override
        public void rollback() {

        }

        @Override
        public <R> Workload<R> submit(Function<Graph, R> function) {
            return null;
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void readWrite() {

        }

        @Override
        public void close() {

        }

        @Override
        public Transaction onReadWrite(Consumer<Transaction> consumer) {
            return null;
        }

        @Override
        public Transaction onClose(Consumer<Transaction> consumer) {
            return null;
        }

        @Override
        public void addTransactionListener(Consumer<Status> consumer) {

        }

        @Override
        public void removeTransactionListener(Consumer<Status> consumer) {

        }

        @Override
        public void clearTransactionListeners() {

        }
    }
}
