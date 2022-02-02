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

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.nio.file.Path;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class ServerSubcommand {

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

    public static class Server extends ServerSubcommand {

        private final boolean isDebug;
        private final boolean isHelp;
        private final boolean isVersion;
        private final Config config;

        Server(boolean isDebug, boolean isHelp, boolean isVersion, Config config) {
            this.isDebug = isDebug;
            this.isHelp = isHelp;
            this.isVersion = isVersion;
            this.config = config;
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

        public Config config() {
            return config;
        }
    }

    public static class Import extends ServerSubcommand {

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

    public static class Export extends ServerSubcommand {

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
