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

package grakn.core.graph.hadoop.scan;

import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphFactory;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.graphdb.olap.VertexJobConverter;
import grakn.core.graph.graphdb.olap.VertexScanJob;
import grakn.core.graph.hadoop.config.JanusGraphHadoopConfiguration;
import grakn.core.graph.hadoop.config.ModifiableHadoopConfiguration;
import grakn.core.graph.hadoop.scan.HadoopScanMapper;

import java.io.IOException;

public class HadoopVertexScanMapper extends HadoopScanMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /* Don't call super implementation super.setup(context); */
        org.apache.hadoop.conf.Configuration hadoopConf = context.getConfiguration();
        ModifiableHadoopConfiguration scanConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, hadoopConf);
        VertexScanJob vertexScan = getVertexScanJob(scanConf);
        ModifiableConfiguration graphConf = getJanusGraphConfiguration(context);
        JanusGraph graph = JanusGraphFactory.open(graphConf);
        job = VertexJobConverter.convert(graph, vertexScan);
        metrics = new HadoopContextScanMetrics(context);
        finishSetup(scanConf, graphConf);
    }

    private VertexScanJob getVertexScanJob(ModifiableHadoopConfiguration conf) {
        String jobClass = conf.get(JanusGraphHadoopConfiguration.SCAN_JOB_CLASS);

        try {
            return (VertexScanJob)Class.forName(jobClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
