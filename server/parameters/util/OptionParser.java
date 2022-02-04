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
 */

package com.vaticle.typedb.core.server.parameters.util;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_FLAG_OPTION_HAS_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRES_TYPED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DUPLICATE_CLI_OPTION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public abstract class OptionParser {

    public final java.lang.String name;
    private final java.lang.String description;
    private final java.lang.String valueDescription;

    public OptionParser(java.lang.String name, java.lang.String description, java.lang.String valueDescription) {
        this.name = name;
        this.description = description;
        this.valueDescription = valueDescription;
    }

    public java.lang.String name() {
        return name;
    }

    java.lang.String description() {
        return description;
    }

    public Help help() {
        return new Help(name, description(), valueDescription);
    }

    FunctionalIterator<Option> matchingOptions(Set<Option> options) {
        return iterate(options).filter(opt -> opt.name().equals(name()));
    }

    public static class Flag extends OptionParser {

        public Flag(java.lang.String name, java.lang.String description) {
            super(name, description, null);
        }

        public boolean parse(Set<Option> options) {
            if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
            Optional<Option> option = matchingOptions(options).first();
            if (option.isEmpty()) return false;
            else if (option.get().hasValue()) throw TypeDBException.of(CLI_FLAG_OPTION_HAS_VALUE, option.get());
            else return true;
        }
    }

    public static class String extends OptionParser {

        private static final java.lang.String valueDescription = "<string>";

        public String(java.lang.String name, java.lang.String description) {
            super(name, description, valueDescription);
        }

        public Optional<java.lang.String> parse(Set<Option> options) {
            if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
            Optional<Option> option = matchingOptions(options).first();
            if (option.isEmpty()) return Optional.empty();
            else if (!option.get().hasValue()) {
                throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get(), valueDescription);
            } else {
                return option.get().stringValue();
            }
        }
    }

    public static class Path extends OptionParser {

        private static final java.lang.String valueDescription = "<path>";

        public Path(java.lang.String name, java.lang.String description) {
            super(name, description, valueDescription);
        }

        public Optional<java.nio.file.Path> parse(Set<Option> options) {
            if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
            Optional<Option> option = matchingOptions(options).first();
            if (option.isEmpty()) return Optional.empty();
            else if (!option.get().hasValue()) {
                throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get(), valueDescription);
            } else {
                return option.get().stringValue().map(Paths::get);
            }
        }
    }

    public static class Int extends OptionParser {

        private static final java.lang.String valueDescription = "<int>";

        public Int(java.lang.String name, java.lang.String description) {
            super(name, description, valueDescription);
        }

        public Optional<Integer> parse(Set<Option> options) {
            if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
            Optional<Option> option = matchingOptions(options).first();
            if (option.isEmpty()) return Optional.empty();
            else if (!option.get().hasValue())
                throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get());
            else {
                try {
                    return Optional.of(Integer.parseInt(option.get().stringValue().get()));
                } catch (NumberFormatException e) {
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get(), valueDescription);
                }
            }
        }
    }

    public static class Help implements com.vaticle.typedb.core.server.parameters.util.Help {

        private final java.lang.String path;
        private final java.lang.String description;
        @Nullable
        private final java.lang.String valueHelp;

        public Help(java.lang.String path, java.lang.String description, @Nullable java.lang.String valueHelp) {
            this.path = path;
            this.description = description;
            this.valueHelp = valueHelp;
        }

        @Override
        public java.lang.String name() {
            return path;
        }

        @Override
        public java.lang.String description() {
            return description;
        }


        @Override
        public java.lang.String toString() {
            if (valueHelp == null) {
                return java.lang.String.format("\t%-60s \t%s\n", (Option.PREFIX + path), description);
            } else {
                return java.lang.String.format("\t%-60s \t%s\n", (Option.PREFIX + path + "=" + valueHelp), description);
            }
        }
    }
}
