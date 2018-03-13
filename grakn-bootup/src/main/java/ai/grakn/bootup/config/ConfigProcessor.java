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

package ai.grakn.bootup.config;

import ai.grakn.engine.GraknConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
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

    public static void updateConfigFromGraknConfig(ProcessConfig config, Path configPath, GraknConfig graknConfig) {
        String updatedConfigString = config
                .updateFromConfig(graknConfig)
                .toConfigString();
        saveConfigStringToFile(updatedConfigString, configPath);
    }
}
