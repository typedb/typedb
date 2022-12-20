/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.common.yaml.YAML;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.util.Option;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRES_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_FILE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.ENV_VAR_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.Constants.CONFIG_PATH;

public class CoreConfigFactory {

    static CoreConfig config(CoreConfigParser parser) {
        return config(new HashSet<>(), parser);
    }

    static CoreConfig config(Set<Option> overrides, CoreConfigParser parser) {
        return config(CONFIG_PATH, overrides, parser);
    }

    static CoreConfig config(Path file, Set<Option> overrides, CoreConfigParser parser) {
        YAML.Map yaml = merge(file, overrides);
        substituteEnvVars(yaml);
        return parser.parse(yaml, "");
    }

    protected static YAML.Map merge(Path file, Set<Option> overrides) {
        YAML.Map yaml = readFile(file);
        YAML.Map yamlOverrides = convertOverrides(overrides);
        for (String key: yamlOverrides.keys()) {
            set(yaml, key, yamlOverrides.get(key));
        }
        return yaml;
    }

    private static YAML.Map readFile(Path file) {
        try {
            YAML config = YAML.load(file);
            if (!config.isMap()) throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP);
            return config.asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(CONFIG_FILE_NOT_FOUND, file);
        }
    }

    private static YAML.Map convertOverrides(Set<Option> options) {
        Set<String> keys = new HashSet<>();
        for (Option option : options) {
            if (!option.hasValue()) throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE, option);
            keys.add(option.name());
        }
        Map<String, YAML> yaml = new HashMap<>();
        for (String key : keys) {
            yaml.put(key, YAML.load(get(options, key)));
        }
        return new YAML.Map(yaml);
    }

    private static String get(Set<Option> source, String key) {
        Set<String> values = iterate(source).filter(opt -> opt.name().equals(key))
                .map(opt -> opt.stringValue().get()).toSet();
        if (values.size() == 1) return values.iterator().next();
        else return "[" + String.join(", ", values) + "]";
    }

    public static void set(YAML.Map destination, String key, YAML value) {
        String[] split = key.split("\\.");
        YAML.Map nested = destination.asMap();
        for (int i = 0; i < split.length - 1; i++) {
            String keyScoped = split[i];
            if (!nested.containsKey(keyScoped)) nested.put(keyScoped, new YAML.Map(new HashMap<>()));
            nested = nested.get(keyScoped).asMap();
        }
        nested.put(split[split.length - 1], value);
    }

    protected static void substituteEnvVars(YAML.Map yaml) {
        for (String key : yaml.keys()) {
            YAML value = yaml.get(key);
            if (value.isString()) {
                String valueStr = value.asString().value();
                if (valueStr.startsWith("$")) {
                    String envVarName = valueStr.substring(1);
                    if (System.getenv(envVarName) == null) throw TypeDBException.of(ENV_VAR_NOT_FOUND, valueStr);
                    else yaml.put(key, YAML.load(System.getenv(envVarName)));
                }
            } else if (value.isMap()) {
                substituteEnvVars(value.asMap());
            }
        }
    }
}
