// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Function;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.Mutation;
import grakn.core.graph.diskstorage.StaticBuffer;

import javax.annotation.Nullable;
import java.util.List;


public class KCVEntryMutation extends Mutation<Entry, Entry> {

    public KCVEntryMutation(List<Entry> additions, List<Entry> deletions) {
        super(additions, deletions);
    }

    public static final Function<Entry, StaticBuffer> ENTRY2COLUMN_FCT = new Function<Entry, StaticBuffer>() {
        @Nullable
        @Override
        public StaticBuffer apply(Entry entry) {
            return entry.getColumn();
        }
    };

    @Override
    public void consolidate() {
        super.consolidate(ENTRY2COLUMN_FCT,ENTRY2COLUMN_FCT);
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(ENTRY2COLUMN_FCT,ENTRY2COLUMN_FCT);
    }

}
