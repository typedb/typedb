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

package com.vaticle.typedb.core.server.parameters.cli;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.parser.cli.Option;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Subcommand {

    public boolean isServer() {
        return false;
    }

    public Server asServer() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Server.class));
    }

    public boolean isImport() {
        return false;
    }

    public Import asImport() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Import.class));
    }

    public boolean isExport() {
        return false;
    }

    public Export asExport() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Export.class));
    }

    public static class Server extends Subcommand {

        private final boolean isDebug;
        private final boolean isHelp;
        private final boolean isVersion;
        @Nullable
        private final Path configPath;
        private final Set<Option> configOptions;

        Server(boolean isDebug, boolean isHelp, boolean isVersion, @Nullable Path configPath, Set<Option> configOptions) {
            this.isDebug = isDebug;
            this.isHelp = isHelp;
            this.isVersion = isVersion;
            this.configPath = configPath;
            this.configOptions = configOptions;
        }

        @Override
        public boolean isServer() {
            return true;
        }

        @Override
        public Server asServer() {
            return this;
        }

        public boolean isDebug() {
            return isDebug;
        }

        public boolean isHelp() {
            return isHelp;
        }

        public boolean isVersion() {
            return isVersion;
        }

        public Optional<Path> configPath() {
            return Optional.ofNullable(configPath);
        }

        public Set<Option> configOptions() {
            return configOptions;
        }
    }

    public static class Import extends Subcommand {

        private final String database;
        private final Path file;
        private final int port;

        Import(String database, Path file, int port) {
            this.database = database;
            this.file = file;
            this.port = port;
        }

        public String database() {
            return database;
        }

        public Path file() {
            return file;
        }

        public int port() {
            return port;
        }

        @Override
        public boolean isImport() {
            return true;
        }

        @Override
        public Import asImport() {
            return this;
        }
    }

    public static class Export extends Subcommand {

        private final String database;
        private final Path file;
        private final int port;

        public Export(String database, Path file, int port) {
            this.database = database;
            this.file = file;
            this.port = port;
        }

        public String database() {
            return database;
        }

        public Path file() {
            return file;
        }

        public int port() {
            return port;
        }

        @Override
        public boolean isExport() {
            return true;
        }

        @Override
        public Export asExport() {
            return this;
        }
    }

}
