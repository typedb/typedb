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

package grakn.core.graph.hadoop.config.job;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract base class for {@link JobClasspathConfigurer}
 * implementations that use Hadoop's distributed cache to store push class files to the cluster.
 */
public abstract class AbstractDistCacheConfigurer {

    private final Conf conf;

    private static final String HDFS_TMP_LIB_DIR = "janusgraphlib";

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDistCacheConfigurer.class);

    public AbstractDistCacheConfigurer(String mapReduceJarFilename) {
        this.conf = configureByClasspath(mapReduceJarFilename);
    }

    public String getMapredJar() {
        return conf.mapReduceJar;
    }

    public ImmutableList<Path> getLocalPaths() {
        return conf.paths;
    }

    protected Path uploadFileIfNecessary(FileSystem localFS, Path localPath, FileSystem destFS) throws IOException {

        // Fast path for local FS -- DistributedCache + local JobRunner seems copy/link files automatically
        if (destFS.equals(localFS)) {
            LOG.debug("Skipping file upload for {} (destination filesystem {} equals local filesystem)", localPath, destFS);
            return localPath;
        }

        Path destPath = new Path(destFS.getHomeDirectory() + "/" + HDFS_TMP_LIB_DIR + "/" + localPath.getName());

        Stats fileStats = null;

        try {
            fileStats = compareModtimes(localFS, localPath, destFS, destPath);
        } catch (IOException e) {
            LOG.warn("Unable to read or stat file: localPath={}, destPath={}, destFS={}",
                    localPath, destPath, destFS);
        }

        if (fileStats != null && !fileStats.isRemoteCopyCurrent()) {
            LOG.debug("Copying {} to {}", localPath, destPath);
            destFS.copyFromLocalFile(localPath, destPath);
            if (null != fileStats.local) {
                final long mtime = fileStats.local.getModificationTime();
                LOG.debug("Setting modtime on {} to {}", destPath, mtime);
                destFS.setTimes(destPath, mtime, -1); // -1 means leave atime alone
            }
        }

        return destPath;
    }

    private Stats compareModtimes(FileSystem localFS, Path localPath, FileSystem destFS, Path destPath) throws IOException {
        Stats s = new Stats();
        s.local = localFS.getFileStatus(localPath);
        if (destFS.exists(destPath)) {
            s.dest = destFS.getFileStatus(destPath);
            if (null != s.dest && null != s.local) {
                long l = s.local.getModificationTime();
                long d = s.dest.getModificationTime();
                if (l == d) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("File {} with modtime {} is up-to-date", destPath, d);
                } else if (l < d) {
                    LOG.warn("File {} has newer modtime ({}) than our local copy {} ({})", destPath, d, localPath, l);
                } else {
                    LOG.debug("Remote file {} exists but is out-of-date: local={} dest={}", destPath, l, d);
                }
            } else {
                LOG.debug("Unable to stat file(s): [LOCAL: path={} stat={}] [DEST: path={} stat={}]",
                        localPath, s.local, destPath, s.dest);
            }
        } else {
            LOG.debug("File {} does not exist", destPath);
        }
        return s;
    }

    private static Conf configureByClasspath(String mapReduceJarFilename) {
        List<Path> paths = new LinkedList<>();
        String classpath = System.getProperty("java.class.path");
        String mrj = mapReduceJarFilename.toLowerCase();
        String mapReduceJarPath = null;
        for (String classPathEntry : classpath.split(File.pathSeparator)) {
            if (classPathEntry.toLowerCase().endsWith(".jar") || classPathEntry.toLowerCase().endsWith(".properties")) {
                paths.add(new Path(classPathEntry));
                if (classPathEntry.toLowerCase().endsWith(mrj)) {
                    mapReduceJarPath = classPathEntry;
                }
            }
        }
        return new Conf(paths, mapReduceJarPath);
    }

    private static class Conf {

        private final ImmutableList<Path> paths;
        private final String mapReduceJar;

        public Conf(List<Path> paths, String mapReduceJar) {
            this.paths = ImmutableList.copyOf(paths);
            this.mapReduceJar = mapReduceJar;
        }
    }


    private static class Stats {
        private FileStatus local;
        private FileStatus dest;

        private boolean isRemoteCopyCurrent() {
            return null != local && null != dest && dest.getModificationTime() == local.getModificationTime();
        }
    }
}
