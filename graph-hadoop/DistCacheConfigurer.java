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

package grakn.core.graph.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import grakn.core.graph.hadoop.config.job.AbstractDistCacheConfigurer;
import grakn.core.graph.hadoop.config.job.JobClasspathConfigurer;

import java.io.IOException;

public class DistCacheConfigurer extends AbstractDistCacheConfigurer implements JobClasspathConfigurer {

    public DistCacheConfigurer(String mapReduceJarFileName) {
        super(mapReduceJarFileName);
    }

    @Override
    public void configure(Job job) throws IOException {

        Configuration conf = job.getConfiguration();
        FileSystem localFS = FileSystem.getLocal(conf);
        FileSystem jobFS = FileSystem.get(conf);

        for (Path p : getLocalPaths()) {
            Path stagedPath = uploadFileIfNecessary(localFS, p, jobFS);
            // Calling this method decompresses the archive and makes Hadoop
            // handle its class files individually.  This leads to crippling
            // overhead times (10+ seconds) even with the LocalJobRunner
            // courtesy of o.a.h.yarn.util.FSDownload.changePermissions
            // copying and changing the mode of each classfile copy file individually.
            //job.addArchiveToClassPath(p);
            // Just add the compressed archive instead:
            job.addFileToClassPath(stagedPath);
        }

        // We don't really need to set a map reduce job jar here,
        // but doing so suppresses a warning
        String mj = getMapredJar();
        if (null != mj)
            job.setJar(mj);
    }
}
