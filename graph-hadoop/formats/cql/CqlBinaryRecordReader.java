// Copyright 2019 JanusGraph Authors
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

package grakn.core.graph.hadoop.formats.cql;

import com.datastax.driver.core.Row;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.cql.CQLKeyColumnValueStore;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;

public class CqlBinaryRecordReader extends RecordReader<StaticBuffer, Iterable<Entry>> {
    private KV currentKV;
    private KV incompleteKV;

    private final GraknCqlRecordReader reader;

    public CqlBinaryRecordReader(GraknCqlRecordReader reader) {
        this.reader = reader;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return null != (currentKV = completeNextKV());
    }

    private KV completeNextKV() throws IOException {
        KV completedKV = null;
        boolean hasNext;
        do {
            hasNext = reader.nextKeyValue();

            if (!hasNext) {
                completedKV = incompleteKV;
                incompleteKV = null;
            } else {
                Row row = reader.getCurrentValue();
                StaticArrayBuffer key = StaticArrayBuffer.of(row.getBytesUnsafe(CQLKeyColumnValueStore.KEY_COLUMN_NAME));
                StaticBuffer column1 = StaticArrayBuffer.of(row.getBytesUnsafe(CQLKeyColumnValueStore.COLUMN_COLUMN_NAME));
                StaticBuffer value = StaticArrayBuffer.of(row.getBytesUnsafe(CQLKeyColumnValueStore.VALUE_COLUMN_NAME));
                Entry entry = StaticArrayEntry.of(column1, value);

                if (null == incompleteKV) {
                    // Initialization; this should happen just once in an instance's lifetime
                    incompleteKV = new KV(key);
                } else if (!incompleteKV.key.equals(key)) {
                    // The underlying Cassandra reader has just changed to a key we haven't seen yet
                    // This implies that there will be no more entries for the prior key
                    completedKV = incompleteKV;
                    incompleteKV = new KV(key);
                }

                incompleteKV.addEntry(entry);
            }
            /* Loop ends when either
             * A) the cassandra reader ran out of data
             * or
             * B) the cassandra reader switched keys, thereby completing a KV */
        } while (hasNext && null == completedKV);

        return completedKV;
    }

    @Override
    public StaticBuffer getCurrentKey() {
        return currentKV.key;
    }

    @Override
    public Iterable<Entry> getCurrentValue() {
        return currentKV.entries;
    }

    @Override
    public void close() {
        reader.close();
    }

    @Override
    public float getProgress() {
        return reader.getProgress();
    }

    private static class KV {
        private final StaticArrayBuffer key;
        private ArrayList<Entry> entries = new ArrayList<>();

        public KV(StaticArrayBuffer key) {
            this.key = key;
        }

        public void addEntry(Entry toAdd) {
            entries.add(toAdd);
        }
    }
}