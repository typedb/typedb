/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.hadoop.formats.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.hadoop.formats.util.input.JanusGraphHadoopSetupImpl;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;


public abstract class HadoopInputFormat extends InputFormat<NullWritable, VertexWritable> implements Configurable, GraphFilterAware {

    private final InputFormat<StaticBuffer, Iterable<Entry>> inputFormat;
    private static final RefCountedCloseable<JanusGraphVertexDeserializer> refCounter;

    static {
        refCounter = new RefCountedCloseable<>((conf) -> new JanusGraphVertexDeserializer(new JanusGraphHadoopSetupImpl(conf)));
    }

    public HadoopInputFormat(InputFormat<StaticBuffer, Iterable<Entry>> inputFormat) {
        this.inputFormat = inputFormat;
        Preconditions.checkState(Configurable.class.isAssignableFrom(inputFormat.getClass()));
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return inputFormat.getSplits(context);
    }

    @Override
    public RecordReader<NullWritable, VertexWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new HadoopRecordReader(refCounter, inputFormat.createRecordReader(split, context));
    }

    @Override
    public void setConf(Configuration conf) {
        ((Configurable) inputFormat).setConf(conf);

        refCounter.setBuilderConfiguration(conf);
    }

    @Override
    public Configuration getConf() {
        return ((Configurable) inputFormat).getConf();
    }

    @Override
    public void setGraphFilter(GraphFilter graphFilter) {
        // do nothing -- loaded via configuration
    }

    public static class RefCountedCloseable<T extends AutoCloseable> {

        private T current;
        private long refCount;
        private final Function<Configuration, T> builder;
        private Configuration configuration;

        public RefCountedCloseable(Function<Configuration, T> builder) {
            this.builder = builder;
        }

        public synchronized void setBuilderConfiguration(Configuration configuration) {
            this.configuration = configuration;
        }

        public synchronized T acquire() {
            if (null == current) {
                Preconditions.checkState(0 == refCount);
                current = builder.apply(configuration);
            }

            refCount++;

            return current;
        }

        public synchronized void release() throws Exception {
            Preconditions.checkNotNull(current);
            Preconditions.checkState(0 < refCount);

            refCount--;

            if (0 == refCount) {
                current.close();
                current = null;
            }
        }
    }
}
