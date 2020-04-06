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

import hypergraph.common.concurrent.ManagedReadWriteLock;
import hypergraph.common.exception.HypergraphException;
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
        private final Map<String, ManagedReadWriteLock> singleLabelLocks;
        private final ManagedReadWriteLock multiLabelLock;

        private Type() {
            typeByLabel = new ConcurrentHashMap<>();
            typeByIID = new ConcurrentHashMap<>();
            singleLabelLocks = new ConcurrentHashMap<>();
            multiLabelLock = new ManagedReadWriteLock();
        }

        public Storage storage() {
            return storage;
        }

        private boolean isInitialised() {
            return get(Schema.Vertex.Type.Root.THING.label()) != null;
        }

        private void initialise() {
            TypeVertex rootType = put(Schema.Vertex.Type.TYPE, Schema.Vertex.Type.Root.THING.label()).setAbstract(true);
            TypeVertex rootEntityType = put(Schema.Vertex.Type.ENTITY_TYPE, Schema.Vertex.Type.Root.ENTITY.label()).setAbstract(true);
            TypeVertex rootAttributeType = put(Schema.Vertex.Type.ATTRIBUTE_TYPE, Schema.Vertex.Type.Root.ATTRIBUTE.label()).setAbstract(true);
            TypeVertex rootRelationType = put(Schema.Vertex.Type.RELATION_TYPE, Schema.Vertex.Type.Root.RELATION.label()).setAbstract(true);
            TypeVertex rootRoleType = put(Schema.Vertex.Type.ROLE_TYPE, Schema.Vertex.Type.Root.ROLE.label()).setAbstract(true);

            rootEntityType.outs().put(Schema.Edge.Type.SUB, rootType);
            rootAttributeType.outs().put(Schema.Edge.Type.SUB, rootType);
            rootRelationType.outs().put(Schema.Edge.Type.SUB, rootType);
            rootRoleType.outs().put(Schema.Edge.Type.SUB, rootType);
        }

        public void commit() {
            typeByIID.values().parallelStream().filter(v -> v.status().equals(Schema.Status.BUFFERED)).forEach(
                    vertex -> vertex.iid(TypeVertex.generateIID(storage.keyGenerator(), vertex.schema()))
            ); // typeByIID no longer contains valid mapping from IID to TypeVertex
            typeByIID.values().parallelStream().forEach(Vertex::commit);
            clear(); // we now flush the indexes after commit, and we do not expect this Graph.Type to be used again
        }

        public TypeVertex update(TypeVertex vertex, String newLabel) {
            try {
                multiLabelLock.lockWrite();
                if (typeByLabel.containsKey(newLabel)) throw new HypergraphException(
                        String.format("Invalid Write Operation: label '%s' is already in use", newLabel));
                typeByLabel.remove(vertex.label());
                typeByLabel.put(newLabel, vertex);
                return vertex;
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                multiLabelLock.unlockWrite();
            }
        }

        public TypeVertex put(Schema.Vertex.Type type, String label) {
            try { // we intentionally use READ on multiLabelLock, as put() only concerns one label
                multiLabelLock.lockRead();
                singleLabelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockWrite();

                TypeVertex typeVertex = typeByLabel.computeIfAbsent(
                        label, l -> new TypeVertex.Buffered(this, type, TypeVertex.generateIID(keyGenerator, type), l)
                );
                typeByIID.put(typeVertex.iid(), typeVertex);
                return typeVertex;
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                singleLabelLocks.get(label).unlockWrite();
                multiLabelLock.unlockRead();
            }
        }

        public TypeVertex get(String label) {
            try {
                multiLabelLock.lockRead();
                singleLabelLocks.computeIfAbsent(label, x -> new ManagedReadWriteLock()).lockRead();

                TypeVertex vertex = typeByLabel.get(label);
                if (vertex != null) return vertex;

                byte[] iid = storage.get(TypeVertex.index(label));
                if (iid != null) {
                    vertex = typeByIID.computeIfAbsent(iid, x -> new TypeVertex.Persisted(this, x, label));
                    typeByLabel.putIfAbsent(label, vertex);
                }

                return vertex;
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                singleLabelLocks.get(label).unlockRead();
                multiLabelLock.unlockRead();
            }
        }

        public TypeVertex get(byte[] iid) {
            TypeVertex vertex = typeByIID.get(iid);
            if (vertex != null) return vertex;

            vertex = typeByIID.computeIfAbsent(iid, x -> new TypeVertex.Persisted(this, x));
            typeByLabel.putIfAbsent(vertex.label(), vertex);

            return vertex;
        }

        public void delete(TypeVertex vertex) {
            try { // we intentionally use READ on multiLabelLock, as delete() only concerns one label
                multiLabelLock.lockRead();
                singleLabelLocks.computeIfAbsent(vertex.label(), x -> new ManagedReadWriteLock()).lockWrite();

                typeByLabel.remove(vertex.label());
                typeByIID.remove(vertex.iid());
            } catch (InterruptedException e) {
                throw new HypergraphException(e);
            } finally {
                singleLabelLocks.get(vertex.label()).unlockWrite();
                multiLabelLock.unlockRead();
            }
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
            ); // typeByIID no longer contains valid mapping from IID to TypeVertex
            thingByIID.values().parallelStream().forEach(Vertex::commit);
            clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
        }

        private void clear() {
            thingByIID.clear();
        }
    }
}
