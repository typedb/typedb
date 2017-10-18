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

import com.google.auto.value.AutoValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

/**
 * @param <T> the type of the values of the key
 * @author Felix Chapman
 */
@AutoValue
public abstract class GraknConfigKey<T> {

    public static final GraknConfigKey<String> VERSION = create("grakn.version");

    public static final GraknConfigKey<String> JWT_SECRET = create("JWT.secret");
    public static final GraknConfigKey<Boolean> PASSWORD_PROTECTED =
            create("password.protected", Boolean::parseBoolean);
    public static final GraknConfigKey<Integer> WEBSERVER_THREADS = create("webserver.threads", Integer::parseInt);
    public static final GraknConfigKey<String> ADMIN_PASSWORD = create("admin.password");

    public static final GraknConfigKey<String> SERVER_HOST_NAME = create("server.host");
    public static final GraknConfigKey<Integer> SERVER_PORT = create("server.port", Integer::parseInt);

    public static final GraknConfigKey<Integer> LOADER_REPEAT_COMMITS =
            create("loader.repeat-commits", Integer::parseInt);

    public static final GraknConfigKey<String> REDIS_HOST = create("queue.host");
    public static final GraknConfigKey<String> REDIS_SENTINEL_HOST = create("redis.sentinel.host");
    public static final GraknConfigKey<String> REDIS_SENTINEL_MASTER = create("redis.sentinel.master");
    public static final GraknConfigKey<Integer> REDIS_POOL_SIZE = create("redis.pool-size", Integer::parseInt);

    public static final GraknConfigKey<String> QUEUE_CONSUMERS = create("queue.consumers");

    public static final GraknConfigKey<Path> STATIC_FILES_PATH = create("server.static-file-dir", Paths::get);

    // Delay for the post processing task in milliseconds
    public static final GraknConfigKey<Integer> POST_PROCESSING_TASK_DELAY =
            create("tasks.postprocessing.delay", Integer::parseInt);
    public static final GraknConfigKey<Integer> TASKS_RETRY_DELAY = create("tasks.retry.delay", Integer::parseInt);

    public static final GraknConfigKey<Long> SHARDING_THRESHOLD =
            create("knowledge-base.sharding-threshold", Long::parseLong);

    public static final GraknConfigKey<Boolean> TEST_START_EMBEDDED_COMPONENTS =
            create("test.start.embedded.components", Boolean::parseBoolean);

    public static GraknConfigKey<String> create(String value) {
        return create(value, Function.identity());
    }

    public static <T> GraknConfigKey<T> create(String value, Function<String, T> parseFunction) {
        return new AutoValue_GraknConfigKey<>(value, parseFunction);
    }

    public abstract String value();

    abstract Function<String, T> parseFunction();

    public final T parse(String value) {
        return parseFunction().apply(value);
    }
}
