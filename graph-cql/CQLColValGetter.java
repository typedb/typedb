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

package grakn.core.graph.diskstorage.cql;

import com.datastax.oss.driver.api.core.cql.Row;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.util.StaticArrayEntry.GetColVal;
import io.vavr.Tuple3;

class CQLColValGetter implements GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> {

    private final EntryMetaData[] schema;

    CQLColValGetter(EntryMetaData[] schema) {
        this.schema = schema;
    }

    @Override
    public StaticBuffer getColumn(Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
        return tuple._1;
    }

    @Override
    public StaticBuffer getValue(Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
        return tuple._2;
    }

    @Override
    public EntryMetaData[] getMetaSchema(Tuple3<StaticBuffer, StaticBuffer, Row> tuple) {
        return this.schema;
    }

    @Override
    public Object getMetaData(Tuple3<StaticBuffer, StaticBuffer, Row> tuple, EntryMetaData metaData) {
        switch (metaData) {
            case TIMESTAMP:
                return tuple._3.getLong(grakn.core.graph.diskstorage.cql.CQLKeyColumnValueStore.WRITETIME_COLUMN_NAME);
            case TTL:
                return tuple._3.getInt(grakn.core.graph.diskstorage.cql.CQLKeyColumnValueStore.TTL_COLUMN_NAME);
            default:
                throw new UnsupportedOperationException("Unsupported meta data: " + metaData);
        }
    }
}