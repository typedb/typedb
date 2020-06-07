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