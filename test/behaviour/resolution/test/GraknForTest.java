package grakn.core.test.behaviour.resolution.test;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

// Taken from Grabl
public class GraknForTest {
    private static final Integer PORT = 48555;
    private Logger logger;
    private final Path baseDir;
    private Path unpackedDir;

    public GraknForTest(Path archive) throws InterruptedException, TimeoutException, IOException {
        this.baseDir = Paths.get(".");
        String archiveRootDir = archive.getFileName().toString().split("\\.")[0];
        unpackedDir = Paths.get(baseDir.toString(), archiveRootDir);
        this.logger = LoggerFactory.getLogger(GraknForTest.class.getSimpleName());
        new ProcessExecutor().command("tar", "-xf", archive.toString(), "-C", baseDir.toString()).execute();
        logger.info("unpacked to: " + unpackedDir.toAbsolutePath());
    }

    public void start() throws IOException, TimeoutException, InterruptedException {
        if (!isPortAvailable(PORT)) {
            throw new IOException(String.format("Port %d is already occupied", PORT));
        }
        logger.info("starting...");
        ProcessResult result = new ProcessExecutor().directory(unpackedDir.toFile()).command("grakn", "server", "start").readOutput(true).execute();
        logger.info("started. here's the output:" + result.getOutput().getUTF8());
    }

    public void stop() throws InterruptedException, TimeoutException, IOException {
        logger.info("stopping...");
        ProcessResult result = new ProcessExecutor().directory(unpackedDir.toFile()).command("grakn", "server", "stop").readOutput(true).execute();
        logger.info("stopped. here's the output:" + result.getOutput().getUTF8());
        logger.info("here are the log files:");
        logger.info("cassandra.log: ");
        logger.info(new String(Files.readAllBytes(Paths.get(unpackedDir.toString(), "logs", "cassandra.log"))));
        logger.info("grakn.log: ");
        logger.info(new String(Files.readAllBytes(Paths.get(unpackedDir.toString(), "logs", "grakn.log"))));
        MoreFiles.deleteRecursively(unpackedDir, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    public Path directory() {
        return unpackedDir;
    }

    private static boolean isPortAvailable(int port) {
        try {
            new ServerSocket(port).close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
