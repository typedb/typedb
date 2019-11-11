/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.util.system;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class ConfigurationUtil {

    private static final char CONFIGURATION_SEPARATOR = '.';

    public static List<String> getUniquePrefixes(Configuration config) {
        final Set<String> nameSet = new HashSet<>();
        final List<String> names = new ArrayList<>();
        final Iterator<String> keyIterator = config.getKeys();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            int pos = key.indexOf(CONFIGURATION_SEPARATOR);
            if (pos > 0) {
                String name = key.substring(0, pos);
                if (nameSet.add(name)) {
                    names.add(name);
                }
            }
        }
        return names;
    }

    public static <T> T instantiate(String className) {
        return instantiate(className, new Object[0], new Class[0]);
    }

    public static <T> T instantiate(String className, Object[] constructorArgs, Class[] classes) {
        Preconditions.checkArgument(constructorArgs != null && classes != null);
        Preconditions.checkArgument(constructorArgs.length == classes.length);
        try {
            Class clazz = Class.forName(className);
            Constructor constructor = clazz.getConstructor(classes);
            return (T) constructor.newInstance(constructorArgs);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find implementation class: " + className, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Implementation class does not have required constructor: " + className, e);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate implementation: " + className, e);
        }
    }

}
