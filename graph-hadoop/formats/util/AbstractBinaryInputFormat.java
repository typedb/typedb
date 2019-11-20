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
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.hadoop.config.JanusGraphHadoopConfiguration;
import grakn.core.graph.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;

public abstract class AbstractBinaryInputFormat extends InputFormat<StaticBuffer, Iterable<Entry>> implements HadoopPoolsConfigurable {

    protected Configuration hadoopConf;
    protected ModifiableHadoopConfiguration mrConf;
    protected BasicConfiguration janusgraphConf;

    @Override
    public void setConf( Configuration config) {
        HadoopPoolsConfigurable.super.setConf(config);
        this.mrConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);
        this.hadoopConf = config;
        this.janusgraphConf = mrConf.getJanusGraphConf();
    }

    @Override
    public Configuration getConf() {
        return hadoopConf;
    }
}
