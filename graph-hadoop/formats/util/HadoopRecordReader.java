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

import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.hadoop.formats.util.HadoopInputFormat;
import grakn.core.graph.hadoop.formats.util.JanusGraphVertexDeserializer;

import java.io.IOException;
import java.util.Optional;

public class HadoopRecordReader extends RecordReader<NullWritable, VertexWritable> {

    private final RecordReader<StaticBuffer, Iterable<Entry>> reader;
    private final HadoopInputFormat.RefCountedCloseable countedDeserializer;
    private JanusGraphVertexDeserializer deserializer;
    private VertexWritable vertex;
    private GraphFilter graphFilter;

    public HadoopRecordReader(HadoopInputFormat.RefCountedCloseable<JanusGraphVertexDeserializer> countedDeserializer, RecordReader<StaticBuffer, Iterable<Entry>> reader) {
        this.countedDeserializer = countedDeserializer;
        this.reader = reader;
        this.deserializer = countedDeserializer.acquire();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);

        Configuration conf = taskAttemptContext.getConfiguration();
        if (conf.get(Constants.GREMLIN_HADOOP_GRAPH_FILTER, null) != null) {
            graphFilter = VertexProgramHelper.deserialize(ConfUtil.makeApacheConfiguration(conf),
                    Constants.GREMLIN_HADOOP_GRAPH_FILTER);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            // TODO janusgraph05 integration -- the duplicate() call may be unnecessary
            TinkerVertex maybeNullTinkerVertex = deserializer.readHadoopVertex(reader.getCurrentKey(), reader.getCurrentValue());
            if (null != maybeNullTinkerVertex) {
                vertex = new VertexWritable(maybeNullTinkerVertex);
                if (graphFilter == null) {
                    return true;
                } else {
                    final Optional<StarGraph.StarVertex> vertexWritable = vertex.get().applyGraphFilter(graphFilter);
                    if (vertexWritable.isPresent()) {
                        vertex.set(vertexWritable.get());
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public VertexWritable getCurrentValue() {
        return vertex;
    }

    @Override
    public void close() throws IOException {
        try {
            deserializer = null;
            countedDeserializer.release();
        } catch (Exception e) {
            throw new IOException(e);
        }
        reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}
