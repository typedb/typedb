/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server.parameters.parser.yml;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.parser.HelpEntry;

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
import static com.vaticle.typedb.core.server.common.Util.scopeKey;

public class YamlParser {

    public static abstract class KeyValue {

        private final String description;

        KeyValue(String description) {
            this.description = description;
        }

        String description() {
            return description;
        }

        public abstract HelpEntry helpEntry(String optionScope);

        public static abstract class Predefined<TYPE> extends KeyValue {

            private final String key;
            final ValueParser<TYPE> valueParser;

            Predefined(String key, String description, ValueParser<TYPE> valueParser) {
                super(description);
                this.key = key;
                this.valueParser = valueParser;
            }

            public String key() {
                return key;
            }

            public abstract TYPE parse(Yaml.Map yaml, String scope);

            public static class Value<TYPE> extends Predefined<TYPE> {

                private Value(String key, String description, ValueParser<TYPE> valueParser) {
                    super(key, description, valueParser);
                }

                public static <TYPE> Value<TYPE> create(String key, String description, ValueParser<TYPE> valueParser) {
                    return new Value<>(key, description, valueParser);
                }

                @Override
                public TYPE parse(Yaml.Map yaml, String scope) {
                    String scopedKey = scopeKey(scope, key());
                    if (!yaml.containsKey(key())) throw TypeDBException.of(MISSING_CONFIG_OPTION, scopedKey);
                    else return valueParser.parse(yaml.get(key()), scopedKey);
                }

                public HelpEntry helpEntry(String optionScope) {
                    String scopedKey = scopeKey(optionScope, key());
                    if (valueParser.isLeaf()) {
                        return new HelpEntry.Simple(scopedKey, description(), valueParser.asPrimitive().help());
                    } else {
                        return new HelpEntry.Yaml.Grouped(scopedKey, description(), valueParser.asCompound().helpEntries(scopedKey));
                    }
                }
            }

            public static class EnumValue<TYPE> extends Predefined<TYPE> {

                private final List<TYPE> values;

                private EnumValue(String key, List<TYPE> values, ValueParser<TYPE> valueParser, String description) {
                    super(key, description, valueParser);
                    this.values = values;
                }

                public static <TYPE> EnumValue<TYPE> create(String key, String description, ValueParser<TYPE> valueParser, List<TYPE> values) {
                    return new EnumValue<>(key, values, valueParser, description);
                }

                @Override
                public TYPE parse(Yaml.Map yaml, String scope) {
                    String scopedKey = scopeKey(scope, key());
                    if (!yaml.containsKey(key())) throw TypeDBException.of(MISSING_CONFIG_OPTION, scopedKey);
                    else {
                        TYPE value = valueParser.parse(yaml.get(key()), scopedKey);
                        if (values.contains(value)) return value;
                        else throw TypeDBException.of(CONFIG_ENUM_UNEXPECTED_VALUE, scopedKey, value, values);
                    }
                }

                @Override
                public HelpEntry helpEntry(String optionScope) {
                    String scopedKey = scopeKey(optionScope, key());
                    if (valueParser.isLeaf()) {
                        String values = String.join("|", iterate(this.values).map(Object::toString).toList());
                        return new HelpEntry.Simple(scopedKey, description(), values);
                    } else {
                        return new HelpEntry.Yaml.Grouped(scopedKey, description(), valueParser.asCompound().helpEntries(scopedKey));
                    }
                }
            }
        }

        public static class Dynamic<TYPE> extends KeyValue {

            private final ValueParser<TYPE> valueParser;

            private Dynamic(String description, ValueParser<TYPE> valueParser) {
                super(description);
                this.valueParser = valueParser;
            }

            public static <TYPE> Dynamic<TYPE> create(String description, ValueParser<TYPE> valueParser) {
                return new Dynamic<>(description, valueParser);
            }

            public Map<String, TYPE> parseFrom(Yaml.Map yaml, String scope, Predefined<?>... exclude) {
                Set<String> excludeKeys = iterate(exclude).map(Predefined::key).toSet();
                Map<String, TYPE> read = new HashMap<>();
                yaml.forEach((key, value) -> {
                    if (!excludeKeys.contains(key)) {
                        String scopedKey = scopeKey(scope, key);
                        read.put(key, valueParser.parse(value, scopedKey));
                    }
                });
                return read;
            }

            @Override
            public HelpEntry helpEntry(String optionScope) {
                if (valueParser.isLeaf()) {
                    return new HelpEntry.Simple(scopeKey(optionScope, "<name>"), description(), valueParser.asPrimitive().help());
                } else {
                    return new HelpEntry.Yaml.Grouped(scopeKey(optionScope, "<name>"), description(), valueParser.asCompound().helpEntries(scopeKey(optionScope, "<name>")));
                }
            }
        }

    }

    public static abstract class ValueParser<TYPE> {

        public abstract TYPE parse(Yaml yaml, String scope);

        boolean isLeaf() {
            return false;
        }

        Primitive<TYPE> asPrimitive() {
            throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Primitive.class));
        }

        boolean isNested() {
            return false;
        }

        Compound<TYPE> asCompound() {
            throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Compound.class));
        }

        public static abstract class Compound<T> extends ValueParser<T> {

            public abstract List<HelpEntry> helpEntries(String scope);

            @Override
            public boolean isNested() {
                return true;
            }

            @Override
            public Compound<T> asCompound() {
                return this;
            }
        }

        public static class Primitive<T> extends ValueParser<T> {
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
            public static final Primitive<Float> FLOAT = new Primitive<>(
                    (yaml) -> yaml.isFloat(),
                    (yaml) -> yaml.asFloat().value(),
                    "<float>"
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
                    (yaml) -> yaml.isList() && iterate(yaml.asList().iterator()).allMatch(Yaml::isString),
                    (yaml) -> iterate(yaml.asList().iterator()).map(elem -> elem.asString().value()).toList(),
                    "<[string, ...]>");

            private final Function<Yaml, Boolean> validator;
            private final Function<Yaml, T> converter;
            private final String help;

            private Primitive(Function<Yaml, Boolean> validator, Function<Yaml, T> converter, String help) {
                this.validator = validator;
                this.converter = converter;
                this.help = help;
            }

            @Override
            public T parse(Yaml yaml, String scope) {
                if (!validator.apply(yaml)) {
                    throw TypeDBException.of(CONFIG_UNEXPECTED_VALUE, scope, yaml, help);
                } else {
                    return converter.apply(yaml);
                }
            }

            @Override
            boolean isLeaf() {
                return true;
            }

            @Override
            Primitive<T> asPrimitive() {
                return this;
            }

            public String help() {
                return help;
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
