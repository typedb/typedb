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

package grakn.core.graph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.MetaAnnotatable;
import grakn.core.graph.diskstorage.MetaAnnotated;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * An index entry is a key-value pair (or field-value pair).
 */

public class IndexEntry implements MetaAnnotated, MetaAnnotatable {

    public final String field;
    public final Object value;

    public IndexEntry(String field, Object value) {
        this(field, value, null);
    }

    public IndexEntry(String field, Object value, Map<EntryMetaData, Object> metadata) {
        Preconditions.checkNotNull(field);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(StringUtils.isNotBlank(field));

        this.field = field;
        this.value = value;

        if (metadata == null || metadata == EntryMetaData.EMPTY_METADATA) {
            return;
        }

        for (Map.Entry<EntryMetaData, Object> e : metadata.entrySet()) {
            setMetaData(e.getKey(), e.getValue());
        }
    }

    //########## META DATA ############
    //copied from StaticArrayEntry

    private Map<EntryMetaData, Object> metadata = EntryMetaData.EMPTY_METADATA;

    @Override
    public synchronized Object setMetaData(EntryMetaData key, Object value) {
        if (metadata == EntryMetaData.EMPTY_METADATA) {
            metadata = new EntryMetaData.Map();
        }

        return metadata.put(key, value);
    }

    @Override
    public boolean hasMetaData() {
        return !metadata.isEmpty();
    }

    @Override
    public Map<EntryMetaData, Object> getMetaData() {
        return metadata;
    }

}
