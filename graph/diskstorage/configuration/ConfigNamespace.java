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
import com.google.common.collect.Maps;

import java.util.Map;

public class ConfigNamespace extends ConfigElement {

    private final boolean isUmbrella;
    private final Map<String, ConfigElement> children = Maps.newHashMap();

    public ConfigNamespace(ConfigNamespace parent, String name, String description, boolean isUmbrella) {
        super(parent,name,description);
        this.isUmbrella=isUmbrella;
    }

    public ConfigNamespace(ConfigNamespace parent, String name, String description) {
        this(parent,name,description,false);
    }

    /**
     * Whether this namespace is an umbrella namespace, that is, is expects immediate sub-namespaces which are user defined.
     */
    public boolean isUmbrella() {
        return isUmbrella;
    }

    /**
     * Whether this namespace or any parent namespace is an umbrella namespace.
     */
    public boolean hasUmbrella() {
        return isUmbrella() || (!isRoot() && getNamespace().hasUmbrella());
    }

    @Override
    public boolean isOption() {
        return false;
    }

    void registerChild(ConfigElement element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(element.getNamespace()==this,"Configuration element registered with wrong namespace");
        Preconditions.checkArgument(!children.containsKey(element.getName()),
                "A configuration element with the same name has already been added to this namespace: %s",element.getName());
        children.put(element.getName(),element);
    }

    public Iterable<ConfigElement> getChildren() {
        return children.values();
    }

    public ConfigElement getChild(String name) {

        ConfigElement child = children.get(name);

        if (null != child) {
            return child;
        }
        return child;
    }

}
