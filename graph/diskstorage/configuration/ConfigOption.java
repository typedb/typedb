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
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.function.Predicate;

public class ConfigOption<O> extends ConfigElement {

    public enum Type {
        /**
         * Once the database has been opened, these configuration options cannot
         * be changed for the entire life of the database
         */
        FIXED,
        /**
         * These options can only be changed for the entire database cluster at
         * once when all instances are shut down
         */
        GLOBAL_OFFLINE,
        /**
         * These options can only be changed globally across the entire database
         * cluster
         */
        GLOBAL,
        /**
         * These options are global but can be overwritten by a local
         * configuration file
         */
        MASKABLE,
        /**
         * These options can ONLY be provided through a local configuration file
         */
        LOCAL
    }

    private final Type type;
    private final Class<O> datatype;
    private final O defaultValue;
    private final Predicate<O> verificationFct;

    public ConfigOption(ConfigNamespace parent, String name, String description, Type type, O defaultValue) {
        this(parent, name, description, type, defaultValue, disallowEmpty());
    }

    public ConfigOption(ConfigNamespace parent, String name, String description, Type type, O defaultValue, Predicate<O> verificationFct) {
        this(parent, name, description, type, (Class<O>) defaultValue.getClass(), defaultValue, verificationFct);
    }

    public ConfigOption(ConfigNamespace parent, String name, String description, Type type, Class<O> dataType) {
        this(parent, name, description, type, dataType, disallowEmpty());
    }

    public ConfigOption(ConfigNamespace parent, String name, String description, Type type, Class<O> dataType, Predicate<O> verificationFct) {
        this(parent, name, description, type, dataType, null, verificationFct);
    }

    public ConfigOption(ConfigNamespace parent, String name, String description, Type type, Class<O> dataType, O defaultValue) {
        this(parent, name, description, type, dataType, defaultValue, disallowEmpty());
    }

    public ConfigOption(ConfigNamespace parent, String name, String description, Type type, Class<O> dataType, O defaultValue, Predicate<O> verificationFct) {
        super(parent, name, description);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(verificationFct);
        this.type = type;
        this.datatype = dataType;
        this.defaultValue = defaultValue;
        this.verificationFct = verificationFct;
    }

    public Type getType() {
        return type;
    }

    public Class<O> getDatatype() {
        return datatype;
    }

    public O getDefaultValue() {
        return defaultValue;
    }

    public boolean isFixed() {
        return type == Type.FIXED;
    }

    public boolean isGlobal() {
        return type == Type.FIXED || type == Type.GLOBAL_OFFLINE || type == Type.GLOBAL || type == Type.MASKABLE;
    }

    public boolean isLocal() {
        return type == Type.MASKABLE || type == Type.LOCAL;
    }

    @Override
    public boolean isOption() {
        return true;
    }

    public O get(Object input) {
        if (input == null) {
            input = defaultValue;
        }
        if (input == null) {
            Preconditions.checkState(verificationFct.test((O) input), "Need to set configuration value: %s", this.toString());
            return null;
        } else {
            return verify(input);
        }
    }

    public O verify(Object input) {
        Preconditions.checkNotNull(input);
        Preconditions.checkArgument(datatype.isInstance(input), "Invalid class for configuration value [%s]. Expected [%s] but given [%s]", this.toString(), datatype, input.getClass());
        O result = (O) input;
        Preconditions.checkArgument(verificationFct.test(result), "Invalid configuration value for [%s]: %s", this.toString(), input);
        return result;
    }


    //########### HELPER METHODS ##################

    private static <O> Predicate<O> disallowEmpty() {
        return o -> {
            if (o == null) {
                return false;
            }
            if (o instanceof String) {
                return StringUtils.isNotBlank((String) o);
            }
            return (!o.getClass().isArray() || (Array.getLength(o) != 0 && Array.get(o, 0) != null))
                    && (!(o instanceof Collection) || (!((Collection) o).isEmpty() && ((Collection) o).iterator().next() != null));
        };
    }

    public static Predicate<Integer> positiveInt() {
        return num -> num != null && num > 0;
    }
}
