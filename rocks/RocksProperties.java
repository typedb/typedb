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
 *
 */

package grakn.rocks;

import java.util.Properties;

public class RocksProperties {

    public final int MEMORY_MINIMUM_RESERVE;
    public final int ATTRIBUTE_SYNC_EXPIRE_DURATION;
    public final int ATTRIBUTE_SYNC_EVICTION_DURATION;

    private final Properties properties;

    RocksProperties(Properties properties) {
        this.properties = properties;
        ATTRIBUTE_SYNC_EXPIRE_DURATION = getOrDefault("attribute-sync.expire-duration", 600);
        ATTRIBUTE_SYNC_EVICTION_DURATION = getOrDefault("attribute-sync.cleanup-duration", 60);
        MEMORY_MINIMUM_RESERVE = getOrDefault("memory.minimum-reserve", 1_000_000_000);
    }

    private int getOrDefault(String key, int defaultValue) {
        if (properties.containsKey(key)) {
            return Integer.parseInt(properties.get(key).toString());
        } else {
            return defaultValue;
        }
    }
}
