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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public interface Configuration {

    boolean has(ConfigOption option, String... umbrellaElements);

    <O> O get(ConfigOption<O> option, String... umbrellaElements);

    Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements);

    Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements);

    Configuration restrictTo(String... umbrellaElements);

    //--------------------

    Configuration EMPTY = new Configuration() {
        @Override
        public boolean has(ConfigOption option, String... umbrellaElements) {
            return false;
        }

        @Override
        public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
            return option.getDefaultValue();
        }

        @Override
        public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
            return Sets.newHashSet();
        }

        @Override
        public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
            return Maps.newHashMap();
        }

        @Override
        public Configuration restrictTo(String... umbrellaElements) {
            return EMPTY;
        }
    };


}
