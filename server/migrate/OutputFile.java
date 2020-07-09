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
