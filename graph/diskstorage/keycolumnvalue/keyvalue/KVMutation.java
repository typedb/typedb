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

package grakn.core.graph.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import grakn.core.graph.diskstorage.Mutation;
import grakn.core.graph.diskstorage.StaticBuffer;

import javax.annotation.Nullable;

/**
 * Mutation for KeyValueStore.
 */
public class KVMutation extends Mutation<KeyValueEntry, StaticBuffer> {

    public KVMutation() {
        super();
    }

    private static final Function<KeyValueEntry, StaticBuffer> ENTRY2KEY_FCT = new Function<KeyValueEntry, StaticBuffer>() {
        @Nullable
        @Override
        public StaticBuffer apply(@Nullable KeyValueEntry keyValueEntry) {
            return keyValueEntry.getKey();
        }
    };

    @Override
    public void consolidate() {
        super.consolidate(ENTRY2KEY_FCT, Functions.identity());
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(ENTRY2KEY_FCT, Functions.identity());
    }

}
