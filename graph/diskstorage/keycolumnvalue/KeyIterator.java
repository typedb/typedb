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

package grakn.core.graph.diskstorage.keycolumnvalue;

import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.util.RecordIterator;


public interface KeyIterator extends RecordIterator<StaticBuffer> {

    /**
     * Returns an iterator over all entries associated with the current
     * key that match the column range specified in the query.
     * <p>
     * Closing the returned sub-iterator has no effect on this iterator.
     *
     * Calling {@link #next()} might close previously returned RecordIterators
     * depending on the implementation, hence it is important to iterate over
     * (and close) the RecordIterator before calling {@link #next()} or {@link #hasNext()}.
     *
     */
    RecordIterator<Entry> getEntries();

}
