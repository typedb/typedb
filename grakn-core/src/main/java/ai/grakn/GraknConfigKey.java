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

package ai.grakn;

import ai.grakn.util.ErrorMessage;
import com.google.auto.value.AutoValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class for keys of properties in the file {@code grakn.properties}.
 *
 * @param <T> the type of the values of the key
 * @author Felix Chapman
 */
@AutoValue
public abstract class GraknConfigKey<T> {

    interface KeyParser<T> {
        Optional<T> parse(Optional<String> string);
    }

    // These are helpful constants to describe how to parse required parameters of certain types.
    private static final KeyParser<String> STRING = str -> str;
    private static final KeyParser<Integer> INT = required(Integer::parseInt);
    private static final KeyParser<Boolean> BOOL = required(Boolean::parseBoolean);
    private static final KeyParser<Long> LONG = required(Long::parseLong);

    public static final GraknConfigKey<Integer> WEBSERVER_THREADS = key("webserver.threads", INT);
    public static final GraknConfigKey<Integer> NUM_BACKGROUND_THREADS = key("background-tasks.threads", INT);

    public static final GraknConfigKey<String> SERVER_HOST_NAME = key("server.host");
    public static final GraknConfigKey<Integer> SERVER_PORT = key("server.port", INT);
    public static final GraknConfigKey<Integer> GRPC_PORT = key("grpc.port", INT);


    public static final GraknConfigKey<List<String>> REDIS_HOST =
            key("queue.host", required(GraknConfigKey::parseCSValue), GraknConfigKey::toStringCSValue);
    public static final GraknConfigKey<List<String>> REDIS_SENTINEL_HOST =
            key("redis.sentinel.host", required(GraknConfigKey::parseCSValue), GraknConfigKey::toStringCSValue);
    public static final GraknConfigKey<String> REDIS_BIND = key("bind");
    public static final GraknConfigKey<String> REDIS_SENTINEL_MASTER = key("redis.sentinel.master");
    public static final GraknConfigKey<Integer> REDIS_POOL_SIZE = key("redis.pool-size", INT);
    public static final GraknConfigKey<Integer> POST_PROCESSOR_POOL_SIZE = key("post-processor.pool-size", INT);
    public static final GraknConfigKey<Integer> POST_PROCESSOR_DELAY = key("post-processor.delay", INT);

    public static final GraknConfigKey<Path> STATIC_FILES_PATH = key("server.static-file-dir", required(Paths::get));

    public static final GraknConfigKey<Integer> SESSION_CACHE_TIMEOUT_MS = key("knowledge-base.schema-cache-timeout-ms", INT);

    public static final GraknConfigKey<Integer> TASKS_RETRY_DELAY = key("tasks.retry.delay", INT);

    public static final GraknConfigKey<Long> SHARDING_THRESHOLD = key("knowledge-base.sharding-threshold", LONG);
    public static final GraknConfigKey<String> KB_MODE = key("knowledge-base.mode");
    public static final GraknConfigKey<String> KB_ANALYTICS = key("knowledge-base.analytics");

    public static final GraknConfigKey<Boolean> TEST_START_EMBEDDED_COMPONENTS =
            key("test.start.embedded.components", BOOL);

    /**
     * The name of the key, how it looks in the properties file
     */
    public abstract String name();

    /**
     * The function used to parse the value of the property.
     */
    abstract KeyParser<T> parser();

    /**
     * The function used to write the property back into the properties file
     */
    abstract Function<T, String> toStringFunction();

    /**
     * Parse the value of a property.
     *
     * This function should return an empty optional if the key was not present and there is no default value.
     *
     * @param value the value of the property. Empty if the property isn't in the property file.
     * @param configFilePath path to the config file
     * @return the parsed value
     *
     * @throws RuntimeException if the value is not present and there is no default value
     */
    public final T parse(Optional<String> value, Path configFilePath) {
        return parser().parse(value).orElseThrow(() ->
                new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(name(), configFilePath))
        );
    }

    /**
     * Convert the value of the property into a string to store in a properties file
     */
    public final String valueToString(T value) {
        return toStringFunction().apply(value);
    }

    /**
     * Create a key for a required string property
     */
    public static GraknConfigKey<String> key(String value) {
        return key(value, STRING);
    }

    /**
     * Create a key with the given parse function
     */
    private static <T> GraknConfigKey<T> key(String value, KeyParser<T> parser) {
        return new AutoValue_GraknConfigKey<>(value, parser, Object::toString);
    }

    /**
     * Create a key with the given parse function and toString function
     */
    private static <T> GraknConfigKey<T> key(String value, KeyParser<T> parser, Function<T, String> toStringFunction) {
        return new AutoValue_GraknConfigKey<>(value, parser, toStringFunction);
    }

    /**
     * A function for parsing a required parameter using the given parse function.
     */
    private static <T> KeyParser<T> required(Function<String, T> parseFunction) {
        return opt -> opt.map(parseFunction);
    }

    private static List<String> parseCSValue(String s) {
        return Arrays.stream(s.split(",")).map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
    }

    private static String toStringCSValue(List<String> values) {
        return values.stream().collect(Collectors.joining(","));
    }

}
