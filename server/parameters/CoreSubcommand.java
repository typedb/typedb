/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.nio.file.Path;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface CoreSubcommand {

    default boolean isServer() {
        return false;
    }

    default Server asServer() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Server.class));
    }

    default boolean isImport() {
        return false;
    }

    default Import asImport() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Import.class));
    }

    default boolean isExport() {
        return false;
    }

    default Export asExport() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Export.class));
    }

    class Server implements CoreSubcommand {

        private final boolean isDebug;
        private final boolean isHelp;
        private final boolean isVersion;
        private final CoreConfig config;

        protected Server(boolean isDebug, boolean isHelp, boolean isVersion, CoreConfig config) {
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

        public CoreConfig config() {
            return config;
        }
    }

    class Import implements CoreSubcommand {

        private final String database;
        private final Path schemaFile;
        private final Path dataFile;
        private final int port;

        protected Import(String database, Path schemaFile, Path dataFile, int port) {
            this.database = database;
            this.schemaFile = schemaFile;
            this.dataFile = dataFile;
            this.port = port;
        }

        public String database() {
            return database;
        }

        public Path schemaFile() {
            return schemaFile;
        }

        public Path dataFile() {
            return dataFile;
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

    class Export implements CoreSubcommand {

        private final String database;
        private final Path schemaFile;
        private final Path dataFile;
        private final int port;

        protected Export(String database, Path schemaFile, Path dataFile, int port) {
            this.database = database;
            this.schemaFile = schemaFile;
            this.dataFile = dataFile;
            this.port = port;
        }

        public String database() {
            return database;
        }

        public Path schemaFile() {
            return schemaFile;
        }

        public Path dataFile() {
            return dataFile;
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
