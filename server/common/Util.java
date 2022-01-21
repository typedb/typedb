/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.server.common;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ENV_VAR_NOT_FOUND;
import static com.vaticle.typedb.core.server.common.Constants.ASCII_LOGO_FILE;

public class Util {

    public static void printASCIILogo() throws IOException {
        if (ASCII_LOGO_FILE.exists()) {
            System.out.println("\n" + new String(Files.readAllBytes(ASCII_LOGO_FILE.toPath()), StandardCharsets.UTF_8));
        }
    }

    public static Path getTypedbDir() {
        String homeDir;
        if ((homeDir = System.getProperty("typedb.dir")) == null) {
            homeDir = System.getProperty("user.dir");
        }
        return Paths.get(homeDir);
    }

    public static Path getConfigPath(Path relativeOrAbsolutePath) {
        if (relativeOrAbsolutePath.isAbsolute()) return relativeOrAbsolutePath;
        else {
            Path typeDBDir = getTypedbDir();
            return typeDBDir.resolve(relativeOrAbsolutePath);
        }
    }

    public static Yaml.Map readConfig(Path path) {
        try {
            Yaml yaml = Yaml.load(path);
            if (!yaml.isMap()) throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP);
            replaceEnvVars(yaml.asMap());
            return yaml.asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(CONFIG_FILE_NOT_FOUND, path);
        }
    }

    private static void replaceEnvVars(Yaml.Map yaml) {
        for (String key : yaml.keys()) {
            if (yaml.get(key).isString()) {
                String value = yaml.get(key).asString().value();
                if (value.startsWith("$")) {
                    String envVarName = value.substring(1);
                    if (System.getenv(envVarName) == null) throw TypeDBException.of(ENV_VAR_NOT_FOUND, value);
                    else yaml.put(key, Yaml.load(System.getenv(envVarName)));
                }
            } else if (yaml.get(key).isMap()) {
                replaceEnvVars(yaml.get(key).asMap());
            }
        }
    }

    public static String scopeKey(String scope, String key) {
        return scope.isEmpty() ? key : scope + "." + key;
    }

    public static void setValue(Yaml.Map yaml, String[] path, Yaml value) {
        Yaml.Map nested = yaml.asMap();
        for (int i = 0; i < path.length - 1; i++) {
            String key = path[i];
            if (!nested.containsKey(key)) nested.put(key, new Yaml.Map(new HashMap<>()));
            nested = nested.get(key).asMap();
        }
        nested.put(path[path.length - 1], value);
    }

}
