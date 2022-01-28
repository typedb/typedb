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

package com.vaticle.typedb.core.server.parameters.config;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.parser.args.Option;

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

public class ConfigFactory {

    public static Config create(ConfigParser parser) {
        return create(new HashSet<>(), parser);
    }

    public static Config create(Set<Option> configFile, ConfigParser parser) {
        return create(CONFIG_PATH, configFile, parser);
    }

    public static Config create(Path configFile, Set<Option> configOverrides, ConfigParser parser) {
        Yaml.Map config = mergeYaml(configFile, configOverrides);
        return parser.parse(config, "");
    }

    private static Yaml.Map mergeYaml(Path yamlFile, Set<Option> yamlOverrides) {
        Yaml.Map config = readYamlFile(yamlFile);
        Yaml.Map overrides = readYamlOverrides(yamlOverrides);
        for (String key: overrides.keys()) {
            set(config, key, overrides.get(key));
        }
        substituteEnvVars(config);
        return config;
    }

    private static Yaml.Map readYamlFile(Path location) {
        try {
            Yaml config = Yaml.load(location);
            if (!config.isMap()) throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP);
            return config.asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(CONFIG_FILE_NOT_FOUND, location);
        }
    }

    private static Yaml.Map readYamlOverrides(Set<Option> options) {
        Set<String> keys = new HashSet<>();
        for (Option option : options) {
            if (!option.hasValue()) throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE, option);
            keys.add(option.name());
        }
        Map<String, Yaml> yaml = new HashMap<>();
        for (String key : keys) {
            yaml.put(key, Yaml.load(get(options, key)));
        }
        return new Yaml.Map(yaml);
    }

    private static String get(Set<Option> source, String key) {
        Set<String> values = iterate(source).filter(opt -> opt.name().equals(key))
                .map(opt -> opt.stringValue().get()).toSet();
        if (values.size() == 1) return values.iterator().next();
        else return "[" + String.join(", ", values) + "]";
    }

    public static void set(Yaml.Map destination, String key, Yaml value) {
        String[] split = key.split("\\.");
        Yaml.Map nested = destination.asMap();
        for (int i = 0; i < split.length - 1; i++) {
            String keyScoped = split[i];
            if (!nested.containsKey(keyScoped)) nested.put(keyScoped, new Yaml.Map(new HashMap<>()));
            nested = nested.get(keyScoped).asMap();
        }
        nested.put(split[split.length - 1], value);
    }

    private static void substituteEnvVars(Yaml.Map yaml) {
        for (String key : yaml.keys()) {
            Yaml value = yaml.get(key);
            if (value.isString()) {
                String valueStr = value.asString().value();
                if (valueStr.startsWith("$")) {
                    String envVarName = valueStr.substring(1);
                    if (System.getenv(envVarName) == null) throw TypeDBException.of(ENV_VAR_NOT_FOUND, valueStr);
                    else yaml.put(key, Yaml.load(System.getenv(envVarName)));
                }
            } else if (value.isMap()) {
                substituteEnvVars(value.asMap());
            }
        }
    }
}
