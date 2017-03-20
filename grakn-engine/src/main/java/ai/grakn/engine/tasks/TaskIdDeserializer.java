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
 */

package ai.grakn.engine.tasks;


import ai.grakn.engine.TaskId;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * <p>
 * Implementation of {@link org.apache.kafka.common.serialization.Deserializer} that allows usage of {@link TaskId} as
 * kafka queue keys
 * </p>
 *
 * @author alexandraorth
 */
public class TaskIdDeserializer implements Deserializer<TaskId> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public TaskId deserialize(String topic, byte[] data) {
        return TaskId.of(new String(data, StandardCharsets.UTF_8));
    }

    @Override
    public void close() {}
}
