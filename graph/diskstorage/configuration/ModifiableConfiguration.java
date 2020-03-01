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

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * This configuration extends BasicConfiguration, adding 'set' and 'remove' capabilities to it.
 * It is also possible to Freeze this Configuration, in order to make it read-only again.
 */
public class ModifiableConfiguration extends BasicConfiguration {

    private static final String FROZEN_KEY = "hidden.frozen";
    private final WriteConfiguration config;
    private Boolean isFrozen;


    public ModifiableConfiguration(ConfigNamespace root, WriteConfiguration config) {
        super(root, config);
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    public <O> ModifiableConfiguration set(ConfigOption<O> option, O value, String... umbrellaElements) {
        String key = super.getPath(option, umbrellaElements);
        value = option.verify(value);
        config.set(key, value);
        return this;
    }

    public void setAll(Map<ConfigElement.PathIdentifier, Object> options) {
        for (Map.Entry<ConfigElement.PathIdentifier, Object> entry : options.entrySet()) {
            Preconditions.checkArgument(entry.getKey().element.isOption());
            set((ConfigOption) entry.getKey().element, entry.getValue(), entry.getKey().umbrellaElements);
        }
    }

    public <O> void remove(ConfigOption<O> option, String... umbrellaElements) {
        Preconditions.checkArgument(!option.isFixed() || !isFrozen(), "Cannot change configuration option: %s", option);
        String key = super.getPath(option, umbrellaElements);
        config.remove(key);
    }

    public void freezeConfiguration() {
        config.set(FROZEN_KEY, Boolean.TRUE);
        if (!isFrozen()) setFrozen();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return config;
    }

    public boolean isFrozen() {
        if (null == isFrozen) {
            Boolean frozen = config.get(FROZEN_KEY, Boolean.class);
            isFrozen = (null == frozen) ? false : frozen;
        }
        return isFrozen;
    }

    private void setFrozen() {
        isFrozen = true;
    }
}
