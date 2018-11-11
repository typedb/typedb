/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.bootup.config;

import grakn.core.util.GraknSystemProperty;
import grakn.core.util.GraknConfig;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Factory class for configs.
 *
 */
public class Configs {

    private static final String STORAGE_CONFIG_PATH = "services/cassandra/";
    private static final String STORAGE_CONFIG_NAME = "cassandra.yaml";

    public static GraknConfig graknConfig(){
        return GraknConfig.read(graknConfigPath());
    }

    public static StorageConfig storageConfig(){
        return StorageConfig.from(storageConfigPath());
    }

    public static Path graknConfigPath(){
        return Paths.get(GraknSystemProperty.CONFIGURATION_FILE.value());
    }

    /** paths relative to dist dir **/

    public static Path storageConfigPath(){ return Paths.get(STORAGE_CONFIG_PATH, STORAGE_CONFIG_NAME); }
}
