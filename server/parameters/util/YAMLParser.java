/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server.parameters.util;

import com.vaticle.typedb.common.yaml.YAML;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_ENUM_UNEXPECTED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_UNEXPECTED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONFIG_OPTION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class YAMLParser {

    public static <TYPE> KeyValue.Predefined<TYPE> predefined(String key, String description, Value<TYPE> valueParser) {
        return new KeyValue.Predefined<>(key, description, valueParser);
    }

    public static <TYPE> KeyValue.Dynamic<TYPE> dynamic(String description, Value<TYPE> valueParser) {
        return new KeyValue.Dynamic<>(description, valueParser);
    }

    public static <TYPE> Value.Restricted<TYPE> restricted(Value.Primitive<TYPE> valueParser, List<TYPE> allowed) {
        return new Value.Restricted<>(valueParser, allowed);
    }

    public static String concatenate(String path, String key) {
        return path.isEmpty() ? key : path + "." + key;
    }

    public static abstract class KeyValue {

        private final String description;

        KeyValue(String description) {
            this.description = description;
        }

        String description() {
            return description;
        }

        public abstract Help help(String path);

        public static class Predefined<TYPE> extends KeyValue {

            private final String key;
            final Value<TYPE> valueParser;

            private Predefined(String key, String description, Value<TYPE> valueParser) {
                super(description);
                this.key = key;
                this.valueParser = valueParser;
            }

            public String key() {
                return key;
            }

            public TYPE parse(YAML.Map yaml, String path) {
                String childPath = concatenate(path, key());
                if (!yaml.containsKey(key())) throw TypeDBException.of(MISSING_CONFIG_OPTION, childPath);
                else return valueParser.parse(yaml.get(key()), childPath);
            }

            public Help help(String path) {
                String childPath = concatenate(path, key());
                if (valueParser.isPrimitive()) {
                    return new Value.Primitive.Help(childPath, description(), valueParser.asPrimitive().help());
                } else if (valueParser.isRestricted()) {
                    return new Value.Primitive.Help(childPath, description(), valueParser.asRestricted().help());
                } else {
                    return new Value.Compound.Help(childPath, description(), valueParser.asCompound().helpList(childPath));
                }
            }
        }

        public static class Dynamic<TYPE> extends KeyValue {

            private final Value<TYPE> valueParser;

            private Dynamic(String description, Value<TYPE> valueParser) {
                super(description);
                this.valueParser = valueParser;
            }

            public Map<String, TYPE> parseFrom(YAML.Map yaml, String path, Predefined<?>... exclude) {
                Set<String> excludeKeys = iterate(exclude).map(Predefined::key).toSet();
                Map<String, TYPE> read = new HashMap<>();
                yaml.forEach((key, value) -> {
                    if (!excludeKeys.contains(key)) {
                        String childPath = concatenate(path, key);
                        read.put(key, valueParser.parse(value, childPath));
                    }
                });
                return read;
            }

            @Override
            public Help help(String path) {
                if (valueParser.isPrimitive()) {
                    return new Value.Primitive.Help(concatenate(path, "<name>"), description(), valueParser.asPrimitive().help());
                } else if (valueParser.isRestricted()) {
                    return new Value.Primitive.Help(concatenate(path, "<name>"), description(), valueParser.asRestricted().help());
                } else {
                    return new Value.Compound.Help(concatenate(path, "<name>"), description(), valueParser.asCompound().helpList(concatenate(path, "<name>")));
                }
            }
        }
    }

    public static abstract class Value<TYPE> {

        public abstract TYPE parse(YAML yaml, String path);

        boolean isPrimitive() {
            return false;
        }

        Primitive<TYPE> asPrimitive() {
            throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Primitive.class));
        }

        public boolean isRestricted() {
            return false;
        }

        Restricted<TYPE> asRestricted() {
            throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Restricted.class));
        }

        boolean isCompound() {
            return false;
        }

        Compound<TYPE> asCompound() {
            throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Compound.class));
        }

        public static abstract class Compound<T> extends Value<T> {

            public abstract List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path);

            @Override
            public boolean isCompound() {
                return true;
            }

            @Override
            public Compound<T> asCompound() {
                return this;
            }

            public static class Help implements com.vaticle.typedb.core.server.parameters.util.Help {

                private final String name;
                private final String description;
                private final List<com.vaticle.typedb.core.server.parameters.util.Help> entries;

                public Help(String name, String description, List<com.vaticle.typedb.core.server.parameters.util.Help> entries) {
                    this.name = name;
                    this.description = description;
                    this.entries = entries;
                }

                @Override
                public String name() {
                    return name;
                }

                @Override
                public String description() {
                    return description;
                }

                @Override
                public String toString() {
                    StringBuilder builder = new StringBuilder();
                    if (hasPrimitiveEntries()) {
                        // only print section headers that have a leaf option in them
                        builder.append(String.format("\n\t### %-50s %s\n", (Option.PREFIX + name + "."), description));
                    }
                    for (com.vaticle.typedb.core.server.parameters.util.Help menu : entries) {
                        builder.append(menu.toString());
                    }
                    return builder.toString();
                }

                private boolean hasPrimitiveEntries() {
                    return iterate(entries).anyMatch(entry -> entry instanceof Primitive.Help);
                }
            }
        }

        public static class Restricted<T> extends Value<T> {
            private final Primitive<T> valueParser;
            private final List<T> allowed;

            private Restricted(Primitive<T> valueParser, List<T> allowed) {
                this.valueParser = valueParser;
                this.allowed = allowed;
            }

            @Override
            public T parse(YAML yaml, String path) {
                T value = valueParser.parse(yaml, path);
                if (allowed.contains(value)) return value;
                else throw TypeDBException.of(CONFIG_ENUM_UNEXPECTED_VALUE, path, value, allowed);
            }

            public String help() {
                return "<" + String.join("+", iterate(this.allowed).map(Object::toString).toList()) + ">";
            }

            @Override
            public boolean isRestricted() {
                return true;
            }

            @Override
            Restricted<T> asRestricted() {
                return this;
            }
        }

        public static class Primitive<T> extends Value<T> {
            public static final Primitive<String> STRING = new Primitive<>(
                    (yaml) -> yaml.isString(),
                    (yaml) -> yaml.asString().value(),
                    "<string>"
            );
            public static final Primitive<Integer> INTEGER = new Primitive<>(
                    (yaml) -> yaml.isInt(),
                    (yaml) -> yaml.asInt().value(),
                    "<int>"
            );
            public static final Primitive<Double> DOUBLE = new Primitive<>(
                    (yaml) -> yaml.isDouble(),
                    (yaml) -> yaml.asDouble().value(),
                    "<double>"
            );
            public static final Primitive<Boolean> BOOLEAN = new Primitive<>(
                    (yaml) -> yaml.isBoolean(),
                    (yaml) -> yaml.asBoolean().value(),
                    "<boolean>"
            );
            public static final Primitive<Path> PATH = new Primitive<>(
                    (yaml) -> yaml.isString(),
                    (yaml) -> Paths.get(yaml.asString().value()),
                    "<relative or absolute path>"
            );
            public static final Primitive<Long> BYTES_SIZE = new Primitive<>(
                    (yaml) -> yaml.isString() && Bytes.isValidSizeString(yaml.asString().value()),
                    (yaml) -> Bytes.parse(yaml.asString().value()),
                    "<size>"
            );
            public static final Primitive<InetSocketAddress> INET_SOCKET_ADDRESS = new Primitive<>(
                    (yaml) -> {
                        if (!yaml.isString()) return false;
                        // use URI to parse IPV4, IVP4 and host names - however, we must add a dummy scheme to use it
                        URI uri = URI.create("scheme://" + yaml.asString().value());
                        return uri.getHost() != null && uri.getPort() != -1;
                    },
                    (yaml) -> {
                        URI uri = URI.create("scheme://" + yaml.asString().value());
                        return new InetSocketAddress(uri.getHost(), uri.getPort());
                    },
                    "<address:port>"
            );
            public static final Primitive<List<String>> LIST_STRING = new Primitive<>(
                    (yaml) -> yaml.isList() && iterate(yaml.asList().iterator()).allMatch(YAML::isString),
                    (yaml) -> iterate(yaml.asList().iterator()).map(elem -> elem.asString().value()).toList(),
                    "<[string, ...]>");

            private final Function<YAML, Boolean> validator;
            private final Function<YAML, T> converter;
            private final String help;

            private Primitive(Function<YAML, Boolean> validator, Function<YAML, T> converter, String help) {
                this.validator = validator;
                this.converter = converter;
                this.help = help;
            }

            @Override
            public T parse(YAML yaml, String path) {
                if (!validator.apply(yaml)) {
                    throw TypeDBException.of(CONFIG_UNEXPECTED_VALUE, path, yaml, help);
                } else {
                    return converter.apply(yaml);
                }
            }

            @Override
            boolean isPrimitive() {
                return true;
            }

            @Override
            Primitive<T> asPrimitive() {
                return this;
            }

            public String help() {
                return help;
            }

            public static class Help implements com.vaticle.typedb.core.server.parameters.util.Help {

                private final String path;
                private final String description;
                private final String valueHelp;

                public Help(String path, String description, String valueHelp) {
                    this.path = path;
                    this.description = description;
                    this.valueHelp = valueHelp;
                }

                @Override
                public String name() {
                    return path;
                }

                @Override
                public String description() {
                    return description;
                }

                @Override
                public String toString() {
                    return String.format("\t%-60s \t%s\n", (Option.PREFIX + path + "=" + valueHelp), description);
                }
            }
        }

        /**
         * Derived from logback FileSize implementation
         */
        private static class Bytes {

            private final static String LENGTH_PART = "([0-9]+)";
            private final static int LENGTH_GROUP = 1;
            private final static String UNIT_PART = "(|kb|mb|gb)?";
            private final static int UNIT_GROUP = 2;
            private static final Pattern FILE_SIZE_PATTERN = Pattern.compile(LENGTH_PART + "\\s*" + UNIT_PART, Pattern.CASE_INSENSITIVE);

            private static long parse(String size) {
                Matcher matcher = FILE_SIZE_PATTERN.matcher(size);
                long coefficient;
                if (matcher.matches()) {
                    String lenStr = matcher.group(LENGTH_GROUP);
                    String unitStr = matcher.group(UNIT_GROUP);
                    long lenValue = Long.parseLong(lenStr);
                    if (unitStr.equalsIgnoreCase("")) coefficient = 1;
                    else if (unitStr.equalsIgnoreCase("kb")) coefficient = com.vaticle.typedb.core.common.collection.Bytes.KB;
                    else if (unitStr.equalsIgnoreCase("mb")) coefficient = com.vaticle.typedb.core.common.collection.Bytes.MB;
                    else if (unitStr.equalsIgnoreCase("gb")) coefficient = com.vaticle.typedb.core.common.collection.Bytes.GB;
                    else throw new IllegalStateException("Unexpected size unit: " + unitStr);
                    return lenValue * coefficient;
                } else {
                    throw new IllegalArgumentException("Size [" + size + "] is not in a recognised format.");
                }
            }

            private static boolean isValidSizeString(String size) {
                Matcher matcher = FILE_SIZE_PATTERN.matcher(size);
                return matcher.matches();
            }
        }
    }
}
