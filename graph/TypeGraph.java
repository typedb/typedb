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
import hypergraph.graph.iid.IndexIID;
import hypergraph.graph.iid.VertexIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.util.Storage;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.impl.TypeVertexImpl;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static hypergraph.graph.iid.VertexIID.Type.generate;
import static hypergraph.graph.util.Schema.Vertex.Type.scopedLabel;

public class TypeGraph implements Graph<VertexIID.Type, TypeVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<String, TypeVertex> typesByLabel;
    private final ConcurrentMap<VertexIID.Type, TypeVertex> typesByIID;
    private final ConcurrentMap<String, ManagedReadWriteLock> singleLabelLocks;
    private final ManagedReadWriteLock multiLabelLock;
    private boolean isModified;

    TypeGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        typesByLabel = new ConcurrentHashMap<>();
        typesByIID = new ConcurrentHashMap<>();
        singleLabelLocks = new ConcurrentHashMap<>();
        multiLabelLock = new ManagedReadWriteLock();
    }

    public ThingGraph thing() {
        return graphManager.thing();
    }

    @Override
    public Storage storage() {
        return graphManager.storage();
    }

    public boolean isInitialised() throws HypergraphException {
        return get(Schema.Vertex.Type.Root.THING.label()) != null;
    }

    public void initialise() throws HypergraphException {
        TypeVertex rootThingType = create(
                Schema.Vertex.Type.THING_TYPE,
                Schema.Vertex.Type.Root.THING.label()).isAbstract(true);
        TypeVertex rootEntityType = create(
                Schema.Vertex.Type.ENTITY_TYPE,
                Schema.Vertex.Type.Root.ENTITY.label()).isAbstract(true);
        TypeVertex rootAttributeType = create(
                Schema.Vertex.Type.ATTRIBUTE_TYPE,
                Schema.Vertex.Type.Root.ATTRIBUTE.label()).isAbstract(true).valueType(Schema.ValueType.OBJECT);
        TypeVertex rootRelationType = create(
                Schema.Vertex.Type.RELATION_TYPE,
                Schema.Vertex.Type.Root.RELATION.label()).isAbstract(true);
        TypeVertex rootRoleType = create(
                Schema.Vertex.Type.ROLE_TYPE,
                Schema.Vertex.Type.Root.ROLE.label(),
                Schema.Vertex.Type.Root.RELATION.label()).isAbstract(true);

        rootEntityType.outs().put(Schema.Edge.Type.SUB, rootThingType);
        rootAttributeType.outs().put(Schema.Edge.Type.SUB, rootThingType);
        rootRelationType.outs().put(Schema.Edge.Type.SUB, rootThingType);
        rootRelationType.outs().put(Schema.Edge.Type.RELATES, rootRoleType);
    }

    @Override
    public Stream<TypeVertex> vertices() {
        return typesByIID.values().stream();
    }

    @Override
    public TypeVertex convert(VertexIID.Type iid) {
        return typesByIID.computeIfAbsent(iid, i -> {
            TypeVertex vertex = new TypeVertexImpl.Persisted(this, i);
            typesByLabel.putIfAbsent(vertex.scopedLabel(), vertex);
            return vertex;
        });
    }

    public TypeVertex get(String label) {
        return get(label, null);
    }

    public TypeVertex get(String label, @Nullable String scope) {
        String scopedLabel = scopedLabel(label, scope);
        try {
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(scopedLabel, x -> new ManagedReadWriteLock()).lockRead();

            TypeVertex vertex = typesByLabel.get(scopedLabel);
            if (vertex != null) return vertex;

            IndexIID.Type index = IndexIID.Type.of(label, scope);
            byte[] iid = graphManager.storage().get(index.bytes());
            if (iid != null) {
                vertex = typesByIID.computeIfAbsent(
                        VertexIID.Type.of(iid), i -> new TypeVertexImpl.Persisted(this, i, label, scope)
                );
                typesByLabel.putIfAbsent(scopedLabel, vertex);
            }

            return vertex;
        } catch (InterruptedException e) {
            throw new HypergraphException(e);
        } finally {
            singleLabelLocks.get(scopedLabel).unlockRead();
            multiLabelLock.unlockRead();
        }
    }

    public TypeVertex create(Schema.Vertex.Type type, String label) {
        return create(type, label, null);
    }

    public TypeVertex create(Schema.Vertex.Type type, String label, @Nullable String scope) {
        String scopedLabel = scopedLabel(label, scope);
        try { // we intentionally use READ on multiLabelLock, as put() only concerns one label
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(scopedLabel, x -> new ManagedReadWriteLock()).lockWrite();

            TypeVertex typeVertex = typesByLabel.computeIfAbsent(scopedLabel, i -> new TypeVertexImpl.Buffered(
                    this, generate(graphManager.keyGenerator(), type), label, scope
            ));
            typesByIID.put(typeVertex.iid(), typeVertex);
            return typeVertex;
        } catch (InterruptedException e) {
            throw new HypergraphException(e);
        } finally {
            singleLabelLocks.get(scopedLabel).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    public TypeVertex update(TypeVertex vertex, String oldLabel, @Nullable String oldScope, String newLabel, @Nullable String newScope) {
        String oldScopedLabel = scopedLabel(oldLabel, oldScope);
        String newScopedLabel = scopedLabel(newLabel, newScope);
        try {
            multiLabelLock.lockWrite();
            if (typesByLabel.containsKey(newScopedLabel)) throw new HypergraphException(
                    String.format("Invalid Write Operation: index '%s' is already in use", newScopedLabel));
            typesByLabel.remove(oldScopedLabel);
            typesByLabel.put(newScopedLabel, vertex);
            return vertex;
        } catch (InterruptedException e) {
            throw new HypergraphException(e);
        } finally {
            multiLabelLock.unlockWrite();
        }
    }

    @Override
    public void delete(TypeVertex vertex) {
        try { // we intentionally use READ on multiLabelLock, as delete() only concerns one label
            multiLabelLock.lockRead();
            singleLabelLocks.computeIfAbsent(vertex.scopedLabel(), x -> new ManagedReadWriteLock()).lockWrite();

            typesByLabel.remove(vertex.scopedLabel());
            typesByIID.remove(vertex.iid());
        } catch (InterruptedException e) {
            throw new HypergraphException(e);
        } finally {
            singleLabelLocks.get(vertex.scopedLabel()).unlockWrite();
            multiLabelLock.unlockRead();
        }
    }

    @Override
    public void setModified() {
        if (!isModified) isModified = true;
    }

    @Override
    public boolean isModified() {
        return isModified;
    }

    /**
     * Commits all the writes captured in this graph into storage.
     *
     * First, for every {@code TypeVertex} that is held in {@code typeByIID},
     * we generate a unique {@code IID} to be persisted in storage. Then, we
     * commit each vertex into storage by calling {@code vertex.commit()}.
     */
    @Override
    public void commit() {
        typesByIID.values().parallelStream().filter(v -> v.status().equals(Schema.Status.BUFFERED)).forEach(
                vertex -> vertex.iid(generate(graphManager.storage().keyGenerator(), vertex.schema()))
        ); // typeByIID no longer contains valid mapping from IID to TypeVertex
        typesByIID.values().forEach(TypeVertex::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Type to be used again
    }

    @Override
    public void clear() {
        typesByIID.clear();
        typesByLabel.clear();
    }
}
