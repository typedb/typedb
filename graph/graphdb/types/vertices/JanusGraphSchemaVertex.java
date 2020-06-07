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

package grakn.core.graph.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.query.vertex.VertexCentricQueryBuilder;
import grakn.core.graph.graphdb.transaction.RelationConstructor;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.SchemaSource;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeDefinitionDescription;
import grakn.core.graph.graphdb.types.TypeDefinitionMap;
import grakn.core.graph.graphdb.types.indextype.CompositeIndexTypeWrapper;
import grakn.core.graph.graphdb.types.indextype.MixedIndexTypeWrapper;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.types.system.BaseLabel;
import grakn.core.graph.graphdb.vertices.CacheVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JanusGraphSchemaVertex extends CacheVertex implements SchemaSource {

    public JanusGraphSchemaVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    private String name = null;

    @Override
    public String name() {
        if (name == null) {
            JanusGraphVertexProperty<String> p;
            if (isLoaded()) {
                StandardJanusGraphTx tx = tx();
                p = (JanusGraphVertexProperty) Iterables.getOnlyElement(RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaName, Direction.OUT),
                        tx), null);
            } else {
                p = Iterables.getOnlyElement(query().type(BaseKey.SchemaName).properties(), null);
            }
            Preconditions.checkNotNull(p, "Could not find type for id: %s", longId());
            name = p.value();
        }
        return JanusGraphSchemaCategory.getName(name);
    }

    @Override
    protected Vertex getVertexLabelInternal() {
        return null;
    }

    private TypeDefinitionMap definition = null;

    @Override
    public TypeDefinitionMap getDefinition() {
        TypeDefinitionMap def = definition;
        if (def == null) {
            def = new TypeDefinitionMap();
            Iterable<JanusGraphVertexProperty> ps;
            if (isLoaded()) {
                StandardJanusGraphTx tx = tx();
                ps = (Iterable) RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaDefinitionProperty, Direction.OUT),
                        tx);
            } else {
                ps = query().type(BaseKey.SchemaDefinitionProperty).properties();
            }
            for (JanusGraphVertexProperty property : ps) {
                TypeDefinitionDescription desc = property.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Preconditions.checkArgument(desc != null && desc.getCategory().isProperty());
                def.setValue(desc.getCategory(), property.value());
            }
            definition = def;
        }
        return def;
    }

    private ListMultimap<TypeDefinitionCategory, Entry> outRelations = null;
    private ListMultimap<TypeDefinitionCategory, Entry> inRelations = null;

    @Override
    public Iterable<Entry> getRelated(TypeDefinitionCategory def, Direction dir) {
        ListMultimap<TypeDefinitionCategory, Entry> relations = dir == Direction.OUT ? outRelations : inRelations;
        if (relations == null) {
            ImmutableListMultimap.Builder<TypeDefinitionCategory, Entry> b = ImmutableListMultimap.builder();
            Iterable<JanusGraphEdge> edges;
            if (isLoaded()) {
                StandardJanusGraphTx tx = tx();
                edges = (Iterable) RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseLabel.SchemaDefinitionEdge, dir),
                        tx);
            } else {
                edges = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir).edges();
            }
            for (JanusGraphEdge edge : edges) {
                JanusGraphVertex oth = edge.vertex(dir.opposite());
                TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Object modifier = null;
                if (desc.getCategory().hasDataType()) {
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(), new Entry((JanusGraphSchemaVertex) oth, modifier));
            }
            relations = b.build();
            if (dir == Direction.OUT) outRelations = relations;
            else inRelations = relations;
        }
        return relations.get(def);
    }

    /**
     * Resets the internal caches used to speed up lookups on this index type.
     * This is needed when the type gets modified in the ManagementSystem.
     */
    @Override
    public void resetCache() {
        name = null;
        definition = null;
        outRelations = null;
        inRelations = null;
    }

    public Iterable<JanusGraphEdge> getEdges(TypeDefinitionCategory def, Direction dir) {
        VertexCentricQueryBuilder queryBuilder = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir);
        return StreamSupport.stream(queryBuilder.edges().spliterator(), false).filter(edge -> {
            TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
            return desc.getCategory() == def;
        }).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public SchemaStatus getStatus() {
        return getDefinition().getValue(TypeDefinitionCategory.STATUS, SchemaStatus.class);
    }

    @Override
    public IndexType asIndexType() {
        Preconditions.checkArgument(getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX), "Schema vertex is not a type vertex: [%s,%s]", longId(), name());
        if (getDefinition().<Boolean>getValue(TypeDefinitionCategory.INTERNAL_INDEX)) {
            return new CompositeIndexTypeWrapper(this);
        } else {
            return new MixedIndexTypeWrapper(this);
        }
    }

}
