/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.common.config;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Class for keys of properties in the file {@code grakn.properties}.
 *
 * @param <T> the type of the values of the key
 */
public class ConfigKey<T> {

    /**
     * Parser for a ConfigKey.
     * Describes how to #read(String) and #write(Object) properties.
     *
     * @param <T> The type of the property value
     */
    public interface KeyParser<T> {

        T read(String string);

        default String write(T value) {
            return value.toString();
        }
    }

    // These are helpful parser to describe how to parse parameters of certain types.
    public static final KeyParser<String> STRING = string -> string;
    public static final KeyParser<Integer> INT = Integer::parseInt;
    public static final KeyParser<Long> LONG = Long::parseLong;
    public static final KeyParser<Boolean> BOOL = Boolean::parseBoolean;
    public static final KeyParser<Path> PATH = Paths::get;

    public static final ConfigKey<String> SERVER_HOST_NAME = key("server.host");
    public static final ConfigKey<Integer> GRPC_PORT = key("grpc.port", INT);

    public static final ConfigKey<String> STORAGE_HOSTNAME = key("storage.hostname");
    public static final ConfigKey<String> STORAGE_BACKEND = key("storage.backend");
    public static final ConfigKey<Integer> STORAGE_PORT = key("storage.port", INT);
    public static final ConfigKey<Integer> HADOOP_STORAGE_PORT = key("janusgraphmr.ioformat.conf.storage.port", INT);
    public static final ConfigKey<String> STORAGE_KEYSPACE = key("storage.cql.keyspace");

    public static final ConfigKey<Long> TYPE_SHARD_THRESHOLD = key("knowledge-base.type-shard-threshold", LONG);
    public static final ConfigKey<String> DATA_DIR = key("data-dir");
    public static final ConfigKey<String> LOG_DIR = key("log.dirs");

    /**
     * The name of the key, how it looks in the properties file
     */
    private final String name;

    /**
     * The parser used to read and write the property.
     */
    private final KeyParser<T> parser;


    public ConfigKey(String value, KeyParser<T> parser) {
        this.name = value;
        this.parser = parser;
    }

    public String name() {
        return name;
    }

    public KeyParser<T> parser() {
        return parser;
    }

    /**
     * Convert the value of the property into a string to store in a properties file
     */
    public final String valueToString(T value) {
        return parser.write(value);
    }


    /**
     * Create a key for a string property
     */
    public static ConfigKey<String> key(String value) {
        return key(value, STRING);
    }

    /**
     * Create a key with the given parser
     */
    public static <T> ConfigKey<T> key(String value, KeyParser<T> parser) {
        return new ConfigKey<>(value, parser);
    }

}
