package com.vaticle.typedb.core.server.parameters.config;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.parser.cli.Option;

import javax.annotation.Nullable;
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

    public static Config create(Set<Option> configOptions, ConfigParser parser) {
        return create(CONFIG_PATH, configOptions, parser);
    }

    public static Config create(@Nullable Path configFilePath, Set<Option> configOptions, ConfigParser parser) {
        Yaml.Map configFile = readConfigFile(configFilePath);
        for (Map.Entry<String, Yaml> configOption: asYaml(configOptions).entrySet()) {
            set(configFile, configOption.getKey().split("\\."), configOption.getValue());
        }
        return parser.parse(configFile, "");
    }

    private static Yaml.Map readConfigFile(Path configFilePath) {
        try {
            Yaml configFile = Yaml.load(configFilePath);
            if (!configFile.isMap()) throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP);
            replaceEnvVars(configFile.asMap());
            return configFile.asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(CONFIG_FILE_NOT_FOUND, configFilePath);
        }
    }

    private static void replaceEnvVars(Yaml.Map configFile) {
        for (String key : configFile.keys()) {
            if (configFile.get(key).isString()) {
                String value = configFile.get(key).asString().value();
                if (value.startsWith("$")) {
                    String envVarName = value.substring(1);
                    if (System.getenv(envVarName) == null) throw TypeDBException.of(ENV_VAR_NOT_FOUND, value);
                    else configFile.put(key, Yaml.load(System.getenv(envVarName)));
                }
            } else if (configFile.get(key).isMap()) {
                replaceEnvVars(configFile.get(key).asMap());
            }
        }
    }

    private static Map<String, Yaml> asYaml(Set<Option> configOptions) {
        Set<String> keys = new HashSet<>();
        for (Option configOption : configOptions) {
            if (!configOption.hasValue()) throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE, configOption);
            keys.add(configOption.name());
        }
        Map<String, Yaml> yaml = new HashMap<>();
        for (String key : keys) {
            yaml.put(key, Yaml.load(get(configOptions, key)));
        }
        return yaml;
    }

    private static String get(Set<Option> configOptions, String key) {
        Set<String> values = iterate(configOptions).filter(opt -> opt.name().equals(key))
                .map(opt -> opt.stringValue().get()).toSet();
        if (values.size() == 1) return values.iterator().next();
        else return "[" + String.join(", ", values) + "]";
    }

    public static void set(Yaml.Map original, String[] path, Yaml value) {
        Yaml.Map nested = original.asMap();
        for (int i = 0; i < path.length - 1; i++) {
            String key = path[i];
            if (!nested.containsKey(key)) nested.put(key, new Yaml.Map(new HashMap<>()));
            nested = nested.get(key).asMap();
        }
        nested.put(path[path.length - 1], value);
    }
}
