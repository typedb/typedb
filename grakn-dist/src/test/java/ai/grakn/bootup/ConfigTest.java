/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.bootup;

import ai.grakn.bootup.config.ConfigProcessor;
import ai.grakn.bootup.config.QueueConfig;
import ai.grakn.bootup.config.StorageConfig;
import ai.grakn.engine.GraknConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigTest {

    private static final String REL_DIR = "src/test/java/ai/grakn/bootup/";
    private static final String STORAGE_INPUT_CONFIG = "storage.yaml";
    private static final String STORAGE_OUTPUT_CONFIG = "cassandra.new.yaml";
    private static final String QUEUE_INPUT_CONFIG = "queue.conf";
    private static final String QUEUE_OUTPUT_CONFIG = "redis.new.conf";

    private static final Path STORAGE_INPUT_PATH = Paths.get(GraknConfig.PROJECT_PATH.toString(), REL_DIR, STORAGE_INPUT_CONFIG);
    private static final Path STORAGE_OUTPUT_PATH = Paths.get(GraknConfig.PROJECT_PATH.toString(), REL_DIR, STORAGE_OUTPUT_CONFIG);
    private static final Path QUEUE_INPUT_PATH = Paths.get(GraknConfig.PROJECT_PATH.toString(), REL_DIR, QUEUE_INPUT_CONFIG);
    private static final Path QUEUE_OUTPUT_PATH = Paths.get(GraknConfig.PROJECT_PATH.toString(), REL_DIR, QUEUE_OUTPUT_CONFIG);

    @Test
    public void testQueueConfigParsing() {
        QueueConfig conf = QueueConfig.from(QUEUE_INPUT_PATH);

        ConfigProcessor.saveConfigStringToFile(conf.toConfigString(), QUEUE_OUTPUT_PATH);
        QueueConfig newConf = QueueConfig.from(QUEUE_OUTPUT_PATH);
        assertEquals(conf.params(), newConf.params());
    }


    @Ignore //too much formatting details
    @Test
    public void testStorageConfigParsing() {
        StorageConfig conf = StorageConfig.from(STORAGE_INPUT_PATH);
        ConfigProcessor.saveConfigStringToFile(conf.toConfigString(), STORAGE_OUTPUT_PATH);
        QueueConfig newConf = QueueConfig.from(STORAGE_OUTPUT_PATH);
        assertEquals(conf.params(), newConf.params());
    }
}