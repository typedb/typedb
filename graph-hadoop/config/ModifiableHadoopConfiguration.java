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

package grakn.core.graph.hadoop.config;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.configuration.ConfigElement;
import grakn.core.graph.diskstorage.configuration.ConfigNamespace;
import grakn.core.graph.diskstorage.configuration.ModifiableConfiguration;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.hadoop.conf.Configuration;

public class ModifiableHadoopConfiguration extends ModifiableConfiguration {

    private final Configuration conf;

    private ModifiableHadoopConfiguration(ConfigNamespace root, Configuration c) {
        super(root, new HadoopConfiguration(c));
        this.conf = c;
    }

    public static ModifiableHadoopConfiguration of(ConfigNamespace root, Configuration c) {
        Preconditions.checkNotNull(c);
        return new ModifiableHadoopConfiguration(root, c);
    }

    private static ModifiableConfiguration prefixView(ModifiableHadoopConfiguration mc) {
        HadoopConfiguration prefixConf = new HadoopConfiguration(mc.getHadoopConfiguration(),
                ConfigElement.getPath(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS, true) + ".");
        return new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, prefixConf);
    }

    private Configuration getHadoopConfiguration() {
        return conf;
    }

    public ModifiableConfiguration getJanusGraphConf() {
        return prefixView(this);
    }
}
