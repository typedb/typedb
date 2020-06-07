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

package grakn.core.graph.diskstorage.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * Read-only configuration which provides a logical union of two Configuration objects
 */
public class MergedConfiguration implements Configuration {

    private final Configuration first;
    private final Configuration second;

    public MergedConfiguration(Configuration first, Configuration second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean has(ConfigOption option, String... umbrellaElements) {
        return first.has(option, umbrellaElements) || second.has(option, umbrellaElements);
    }

    @Override
    public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
        if (first.has(option, umbrellaElements)) {
            return first.get(option, umbrellaElements);
        }

        if (second.has(option, umbrellaElements)) {
            return second.get(option, umbrellaElements);
        }

        return option.getDefaultValue();
    }

    @Override
    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
        ImmutableSet.Builder<String> b = ImmutableSet.builder();
        b.addAll(first.getContainedNamespaces(umbrella, umbrellaElements));
        b.addAll(second.getContainedNamespaces(umbrella, umbrellaElements));
        return b.build();
    }

    @Override
    public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
        ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
        Map<String, Object> fm = first.getSubset(umbrella, umbrellaElements);
        Map<String, Object> sm = second.getSubset(umbrella, umbrellaElements);

        b.putAll(first.getSubset(umbrella, umbrellaElements));

        for (Map.Entry<String, Object> secondEntry : sm.entrySet()) {
            if (!fm.containsKey(secondEntry.getKey())) {
                b.put(secondEntry);
            }
        }

        return b.build();
    }

    @Override
    public Configuration restrictTo(String... umbrellaElements) {
        return new MergedConfiguration(first.restrictTo(umbrellaElements), second.restrictTo(umbrellaElements));
    }
}
