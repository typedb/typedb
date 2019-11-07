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
