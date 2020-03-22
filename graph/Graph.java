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

package hypergraph.graph;

import hypergraph.common.HypergraphException;
import hypergraph.common.ManagedReadWriteLock;
import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Graph {

    private final Storage storage;
    private final KeyGenerator keyGenerator;
    private final Graph.Type typeGraph;
    private final Graph.Thing thingGraph;

    public Graph(Storage storage) {
        this.storage = storage;
        keyGenerator = new KeyGenerator(Schema.Key.BUFFERED);
        typeGraph = new Graph.Type();
        thingGraph = new Graph.Thing();
    }

    public Storage storage() {
        return storage;
    }

    public Graph.Type type() {
        return typeGraph;
    }

    public Graph.Thing thing() {
        return thingGraph;
    }

    public void clear() {
        typeGraph.clear();
        thingGraph.clear();
    }

    public boolean isInitialised() {
        return typeGraph.isInitialised();
    }

    public void initialise() {
        typeGraph.initialise();
    }

    public void commit() {
        typeGraph.commit();
        thingGraph.commit();
    }

    public class Type {

        private final Map<String, TypeVertex> typeByLabel;
        private final Map<byte[], TypeVertex> typeByIID;
        private final Map<String, ManagedReadWriteLock> labelLocks;

        private Type() {
            typeByLabel = new ConcurrentHashMap<>();
            typeByIID = new ConcurrentHashMap<>();
            labelLocks = new ConcurrentHashMap<>();
        }

        public Storage storage() {
            return storage;
        }

        private boolean isInitialised() {
            return getVertex(Schema.Vertex.Type.Root.THING.label()) != null;
        }

        private void initialise() {
            TypeVertex rootType = createVertex(Schema.Vertex.Type.TYPE, Schema.Vertex.Type.Root.THING.label()).setAbstract(true);
            TypeVertex rootEntityType = createVertex(Schema.Vertex.Type.ENTITY_TYPE, Schema.Vertex.Type.Root.ENTITY.label()).setAbstract(true);
            TypeVertex rootRelationType = createVertex(Schema.Vertex.Type.RELATION_TYPE, Schema.Vertex.Type.Root.RELATION.label()).setAbstract(true);
            TypeVertex rootRoleType = createVertex(Schema.Vertex.Type.ROLE_TYPE, Schema.Vertex.Type.Root.ROLE.label()).setAbstract(true);
            TypeVertex rootAttributeType = createVertex(Schema.Vertex.Type.ATTRIBUTE_TYPE, Schema.Vertex.Type.Root.ATTRIBUTE.label()).setAbstract(true);

            createEdge(Schema.Edge.Type.SUB, rootEntityType, rootType);
            createEdge(Schema.Edge.Type.SUB, rootRelationType, rootType);
            createEdge(Schema.Edge.Type.SUB, rootRoleType, rootType);
            createEdge(Schema.Edge.Type.SUB, rootAttributeType, rootType);
        }

        public void commit() {
            typeByIID.values().parallelStream().filter(v -> v.status().equals(Schema.Status.BUFFERED)).forEach(
                    vertex -> vertex.iid(TypeVertex.generateIID(storage.keyGenerator(), vertex.schema()))
            );
            typeByIID.values().parallelStream().forEach(Vertex::commit);
        }

        public TypeEdge createEdge(Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            TypeEdge edge = new TypeEdge.Buffered(this, schema, from, to);
            from.out(edge);
            to.in(edge);
            return edge;
        }

        public TypeVertex createVertex(Schema.Vertex.Type type, String label) {
            try {
                labelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockWrite();
                TypeVertex typeVertex = typeByLabel.computeIfAbsent(
                        label, l -> new TypeVertex.Buffered(this, type, TypeVertex.generateIID(keyGenerator, type), l)
                );
                typeByIID.put(typeVertex.iid(), typeVertex);
                return typeVertex;
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                labelLocks.get(label).unlockWrite();
                labelLocks.remove(label);
            }
        }

        public TypeVertex getVertex(String label) {
            try {
                labelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockRead();

                TypeVertex vertex = typeByLabel.get(label);
                if (vertex != null) return vertex;

                byte[] iid = storage.get(TypeVertex.generateIndex(label));
                if (iid != null) {
                    vertex = typeByIID.computeIfAbsent(iid, x -> new TypeVertex.Persisted(this, x, label));
                    typeByLabel.putIfAbsent(label, vertex);
                }

                return vertex;
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                labelLocks.get(label).unlockRead();
                labelLocks.remove(label);
            }
        }

        public TypeVertex getVertex(byte[] iid) {
            TypeVertex vertex = typeByIID.get(iid);
            if (vertex != null) return vertex;

            vertex = typeByIID.computeIfAbsent(iid, x -> new TypeVertex.Persisted(this, x));
            typeByLabel.putIfAbsent(vertex.label(), vertex);

            return vertex;
        }

        private void clear() {
            typeByIID.clear();
            typeByLabel.clear();
        }
    }

    public class Thing {

        private final Map<byte[], ThingVertex> thingByIID;

        Thing() {
            thingByIID = new ConcurrentHashMap<>();
        }

        public void commit() {
            thingByIID.values().parallelStream().forEach(
                    vertex -> vertex.iid(ThingVertex.generateIID(storage.keyGenerator(), vertex.schema()))
            );
            thingByIID.values().parallelStream().forEach(Vertex::commit);
        }

        private void clear() {
            thingByIID.clear();
        }
    }
}
