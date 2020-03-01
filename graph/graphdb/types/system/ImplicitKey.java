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

package grakn.core.graph.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.JanusGraphProperty;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.graphdb.internal.InternalElement;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.InternalVertexLabel;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Token;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class ImplicitKey extends EmptyRelationType implements SystemRelationType, PropertyKey {

    public static final ImplicitKey ID = new ImplicitKey(1001, T.id.getAccessor(), Object.class);

    public static final ImplicitKey JANUSGRAPHID = new ImplicitKey(1002, Token.makeSystemName("nid"), Long.class);

    public static final ImplicitKey LABEL = new ImplicitKey(11, T.label.getAccessor(), String.class);

    public static final ImplicitKey KEY = new ImplicitKey(12, T.key.getAccessor(), String.class);

    public static final ImplicitKey VALUE = new ImplicitKey(13, T.value.getAccessor(), Object.class);

    public static final ImplicitKey ADJACENT_ID = new ImplicitKey(1003, Token.makeSystemName("adjacent"), Long.class);

    //######### IMPLICIT KEYS WITH ID ############

    public static final ImplicitKey TIMESTAMP = new ImplicitKey(5, Token.makeSystemName("timestamp"), Instant.class);

    public static final ImplicitKey VISIBILITY = new ImplicitKey(6, Token.makeSystemName("visibility"), String.class);

    public static final ImplicitKey TTL = new ImplicitKey(7, Token.makeSystemName("ttl"), Duration.class);


    public static final Map<EntryMetaData, ImplicitKey> MetaData2ImplicitKey = ImmutableMap.of(
            EntryMetaData.TIMESTAMP, TIMESTAMP,
            EntryMetaData.TTL, TTL,
            EntryMetaData.VISIBILITY, VISIBILITY);

    private final Class<?> datatype;
    private final String name;
    private final long id;

    private ImplicitKey(long id, String name, Class<?> datatype) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name) && datatype != null && id > 0);
        this.datatype = datatype;
        this.name = name;
        this.id = BaseRelationType.getSystemTypeId(id, JanusGraphSchemaCategory.PROPERTYKEY);
    }


    public <O> O computeProperty(InternalElement e) {
        if (this == ID) {
            return (O) e.id();
        } else if (this == JANUSGRAPHID) {
            return (O) Long.valueOf(e.longId());
        } else if (this == LABEL) {
            return (O) e.label();
        } else if (this == KEY) {
            if (e instanceof JanusGraphProperty) return (O) ((JanusGraphProperty) e).key();
            else return null;
        } else if (this == VALUE) {
            if (e instanceof JanusGraphProperty) return (O) ((JanusGraphProperty) e).value();
            else return null;
        } else if (this == TIMESTAMP || this == VISIBILITY) {
            if (e instanceof InternalRelation) {
                InternalRelation r = (InternalRelation) e;
                if (this == VISIBILITY) {
                    return r.getValueDirect(this);
                } else {
                    Long time = r.getValueDirect(this);
                    if (time == null) return null; //there is no timestamp
                    return (O) r.tx().getConfiguration().getTimestampProvider().getTime(time);
                }
            } else {
                return null;
            }
        } else if (this == TTL) {
            int ttl;
            if (e instanceof InternalRelation) {
                ttl = ((InternalRelationType) ((InternalRelation) e).getType()).getTTL();
            } else if (e instanceof InternalVertex) {
                ttl = ((InternalVertexLabel) ((InternalVertex) e).vertexLabel()).getTTL();
            } else {
                ttl = 0;
            }
            return (O) Duration.ofSeconds(ttl);
        } else throw new AssertionError("Implicit key property is undefined: " + this.name());
    }

    @Override
    public Class<?> dataType() {
        return datatype;
    }

    @Override
    public Cardinality cardinality() {
        return Cardinality.SINGLE;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isPropertyKey() {
        return true;
    }

    @Override
    public boolean isEdgeLabel() {
        return false;
    }

    @Override
    public boolean isInvisibleType() {
        return false;
    }

    @Override
    public Multiplicity multiplicity() {
        return Multiplicity.convert(cardinality());
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        return ConsistencyModifier.DEFAULT;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir == Direction.OUT;
    }

    @Override
    public long longId() {
        return id;
    }

    @Override
    public boolean hasId() {
        return id > 0;
    }

    @Override
    public void setId(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }

    @Override
    public String toString() {
        return name;
    }

}
