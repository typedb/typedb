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

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Base64;
import java.util.Map;

/**
 * <p>
 * Implementation of {@link org.apache.kafka.common.serialization.Serializer} that allows usage of {@link TaskState} as
 * kafka queue values
 * </p>
 *
 * @author alexandraorth
 */
public class TaskStateDeserializer implements Deserializer<TaskState> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public TaskState deserialize(String topic, byte[] data) {
        return (TaskState) SerializationUtils.deserialize(data);
    }

    @Override
    public void close() {}

    public static TaskState deserializeFromString(String data){
        return (TaskState) SerializationUtils.deserialize(Base64.getMimeDecoder().decode(data));
    }
}
