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

package grakn.core.graph.graphdb.database.serialize.attribute;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

import java.time.Duration;


public class DurationSerializer implements AttributeSerializer<Duration> {

    private final LongSerializer secondsSerializer = new LongSerializer();
    private final IntegerSerializer nanosSerializer = new IntegerSerializer();

    @Override
    public Duration read(ScanBuffer buffer) {
        long seconds = secondsSerializer.read(buffer);
        long nanos = nanosSerializer.read(buffer);
        return Duration.ofSeconds(seconds, nanos);
    }

    @Override
    public void write(WriteBuffer buffer, Duration attribute) {
        secondsSerializer.write(buffer,attribute.getSeconds());
        nanosSerializer.write(buffer,attribute.getNano());
    }
}
