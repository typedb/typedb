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

package ai.grakn.engine.bootup.config;

import ai.grakn.engine.GraknConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Helper class for updating storage config file.
 *
 * @author Kasper Piskorski
 */
public class ConfigProcessor {

    public static String getConfigStringFromFile(Path configPath){
        try {
            byte[] bytes = Files.readAllBytes(configPath);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void saveConfigStringToFile(String configString, Path configPath){
        try {
            Files.write(configPath, configString.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateStorageConfig(){
        GraknConfig graknConfig = Configs.graknConfig();
        String updatedStorageConfigString = Configs.storageConfig()
                .updateFromConfig(graknConfig)
                .toConfigString();
        saveConfigStringToFile(updatedStorageConfigString, Configs.storageConfigPath());
    }

    public static void updateProcessConfigs() {
        updateStorageConfig();
    }
}
