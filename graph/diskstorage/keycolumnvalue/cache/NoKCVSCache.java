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

import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;

import java.util.List;


public class NoKCVSCache extends KCVSCache {

    public NoKCVSCache(KeyColumnValueStore store) {
        super(store, null);
    }

    @Override
    public void clearCache() {
    }

    @Override
    protected void invalidate(StaticBuffer key, List<StaticBuffer> entries) {
    }

}
