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

package grakn.core.server.migrate;

import grakn.core.server.migrate.proto.DataProto;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class OutputFile implements Output, AutoCloseable {
    private final OutputStream outputStream;

    public OutputFile(Path path) throws IOException {
        Files.createDirectories(path.getParent());

        outputStream = new BufferedOutputStream(Files.newOutputStream(path));
    }

    @Override
    public synchronized void write(DataProto.Item item) throws IOException {
        item.writeDelimitedTo(outputStream);
    }

    @Override
    public void close() throws IOException {
        outputStream.flush();
        outputStream.close();
    }
}
