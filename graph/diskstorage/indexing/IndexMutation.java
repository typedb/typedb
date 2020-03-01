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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.Mutation;

import java.util.AbstractMap;
import java.util.List;

/**
 * An index mutation contains the field updates (additions and deletions) for a particular index entry.
 * In addition it maintains two boolean values: 1) isNew - the entry is newly created, 2) isDeleted -
 * the entire entry is being deleted. These can be used by an IndexProvider to execute updates more
 * efficiently.
 */

public class IndexMutation extends Mutation<IndexEntry, IndexEntry> {

    private final KeyInformation.StoreRetriever storeRetriever;
    private final boolean isNew;
    private boolean isDeleted;

    private final Function<IndexEntry, Object> entryConversionFunction =
            indexEntry -> isCollection(indexEntry.field) ?
                    new AbstractMap.SimpleEntry<>(indexEntry.field, indexEntry.value) :
                    indexEntry.field;

    public IndexMutation(KeyInformation.StoreRetriever storeRetriever, boolean isNew, boolean isDeleted) {
        super();
        Preconditions.checkArgument(!(isNew && isDeleted), "Invalid status");
        this.storeRetriever = storeRetriever;
        this.isNew = isNew;
        this.isDeleted = isDeleted;
    }

    public void merge(IndexMutation m) {
        Preconditions.checkArgument(isNew == m.isNew, "Incompatible new status");
        Preconditions.checkArgument(isDeleted == m.isDeleted, "Incompatible delete status");
        super.merge(m);
    }

    public boolean isNew() {
        return isNew;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void resetDelete() {
        isDeleted = false;
    }

    private boolean isCollection(String field) {
        KeyInformation keyInformation = storeRetriever.get(field);
        return keyInformation != null && keyInformation.getCardinality() != Cardinality.SINGLE;
    }

    @Override
    public void consolidate() {
        super.consolidate(entryConversionFunction, entryConversionFunction);
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(entryConversionFunction, entryConversionFunction);
    }

    public int determineTTL() {
        return hasDeletions() ? 0 : determineTTL(getAdditions());
    }

    private static int determineTTL(List<IndexEntry> additions) {
        if (additions == null || additions.isEmpty()) {
            return 0;
        }

        int ttl = -1;
        for (IndexEntry add : additions) {
            int ittl = 0;
            if (add.hasMetaData()) {
                Preconditions.checkArgument(add.getMetaData().size() == 1 && add.getMetaData().containsKey(EntryMetaData.TTL),
                        "Index only supports TTL meta data. Found: %s", add.getMetaData());
                ittl = (Integer) add.getMetaData().get(EntryMetaData.TTL);
            }
            if (ttl < 0) {
                ttl = ittl;
            }
            Preconditions.checkArgument(ttl == ittl, "Index only supports uniform TTL values across all " +
                    "index fields, but got additions: %s", additions);
        }
        return ttl;
    }

}
