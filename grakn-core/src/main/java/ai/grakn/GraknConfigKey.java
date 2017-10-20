/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
 *
 */

package ai.grakn;

import ai.grakn.util.ErrorMessage;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @param <T> the type of the values of the key
 * @author Felix Chapman
 */
@AutoValue
public abstract class GraknConfigKey<T> {

    private static final Function<Optional<String>, Optional<Integer>> INT = required(Integer::parseInt);
    private static final Function<Optional<String>, Optional<Boolean>> BOOL = required(Boolean::parseBoolean);
    private static final Function<Optional<String>, Optional<Long>> LONG = required(Long::parseLong);
    private static final Function<Optional<String>, Optional<Path>> PATH = required(Paths::get);

    public static final GraknConfigKey<String> VERSION = key("grakn.version");

    public static final GraknConfigKey<Optional<String>> JWT_SECRET = key("JWT.secret", Optional::of);
    public static final GraknConfigKey<Boolean> PASSWORD_PROTECTED = key("password.protected", BOOL);
    public static final GraknConfigKey<Integer> WEBSERVER_THREADS = key("webserver.threads", INT);
    public static final GraknConfigKey<String> ADMIN_PASSWORD = key("admin.password");

    public static final GraknConfigKey<String> SERVER_HOST_NAME = key("server.host");
    public static final GraknConfigKey<Integer> SERVER_PORT = key("server.port", INT);

    public static final GraknConfigKey<Integer> LOADER_REPEAT_COMMITS = key("loader.repeat-commits", INT);

    public static final GraknConfigKey<List<String>> REDIS_HOST =
            key("queue.host", required(GraknConfigKey::parseCSValue));
    public static final GraknConfigKey<List<String>> REDIS_SENTINEL_HOST =
            key("redis.sentinel.host", withDefault(GraknConfigKey::parseCSValue, ImmutableList.of()));
    public static final GraknConfigKey<String> REDIS_SENTINEL_MASTER =
            key("redis.sentinel.master", withDefault(Function.identity(), "graknmaster"));
    public static final GraknConfigKey<Integer> REDIS_POOL_SIZE = key("redis.pool-size", INT);
    public static final GraknConfigKey<Integer> QUEUE_CONSUMERS = key("queue.consumers", INT);

    public static final GraknConfigKey<Path> STATIC_FILES_PATH = key("server.static-file-dir", PATH);

    // Delay for the post processing task in milliseconds
    public static final GraknConfigKey<Integer> POST_PROCESSING_TASK_DELAY = key("tasks.postprocessing.delay", INT);
    public static final GraknConfigKey<Integer> TASKS_RETRY_DELAY = key("tasks.retry.delay", INT);

    public static final GraknConfigKey<Long> SHARDING_THRESHOLD = key("knowledge-base.sharding-threshold", LONG);

    public static final GraknConfigKey<Boolean> TEST_START_EMBEDDED_COMPONENTS =
            key("test.start.embedded.components", BOOL);

    public static GraknConfigKey<String> key(String value) {
        return key(value, Function.identity());
    }

    public static <T> GraknConfigKey<T> key(String value, Function<Optional<String>, Optional<T>> parseFunction) {
        return new AutoValue_GraknConfigKey<>(value, parseFunction);
    }

    private static <T> Function<Optional<String>, Optional<T>> required(Function<String, T> parseFunction) {
        return opt -> opt.map(parseFunction);
    }

    private static <T> Function<Optional<String>, Optional<T>> withDefault(Function<String, T> parseFunction, T def) {
        return opt -> Optional.of(opt.map(parseFunction).orElse(def));
    }

    public abstract String value();

    abstract Function<Optional<String>, Optional<T>> parseFunction();

    public final T parse(Optional<String> value, Path configFilePath) {
        return parseFunction().apply(value).orElseThrow(() ->
                new RuntimeException(ErrorMessage.UNAVAILABLE_PROPERTY.getMessage(value, configFilePath))
        );
    }

    private static List<String> parseCSValue(String s) {
        return Arrays.stream(s.split(",")).map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
    }
}
