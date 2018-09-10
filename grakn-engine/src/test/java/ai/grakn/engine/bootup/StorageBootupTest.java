package ai.grakn.engine.bootup;

import org.apache.cassandra.service.CassandraDaemon;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;
import org.zeroturnaround.exec.listener.ProcessListener;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static ai.grakn.engine.bootup.BootupProcessExecutor.WAIT_INTERVAL_SECOND;

public class StorageBootupTest {
    private static final Path STORAGE_PIDFILE = Paths.get(System.getProperty("java.io.tmpdir"), "grakn-storage.pid");
    @Test
    public void test() {
        try {
            exec(CassandraDaemon.class);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void exec(Class klass) throws IOException,
            InterruptedException {
        String classpath = "C:\\Users\\Grakn Labs\\Desktop\\grakn-core-1.4.0-SNAPSHOT\\services\\lib\\*";
        String className = klass.getCanonicalName();

        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

            Path graknHome = Paths.get("C:", "Users", "Grakn Labs", "Desktop", "grakn-core-1.4.0-SNAPSHOT");
            Path cassandraConfig = graknHome.resolve("services").resolve("cassandra").resolve("cassandra.yaml");
            Path logDirectory = graknHome.resolve("logs");
            Path logback = graknHome.resolve("services").resolve("cassandra").resolve("logback.xml");


            Future<ProcessResult> result = new ProcessExecutor()
                    .readOutput(true)
                    .directory(graknHome.toFile())
                    .redirectError(stderr)
                    .addListener(new ProcessListener() {
                        @Override
                        public void afterStart(Process process, ProcessExecutor executor) {
                            super.afterStart(process, executor);
                            String pidString = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
                            try{
                                PrintWriter writer = new PrintWriter(STORAGE_PIDFILE.toString(), "UTF-8");
                                writer.println(pidString);
                                writer.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void afterStop(Process process) {
                            super.afterStop(process);
                            STORAGE_PIDFILE.toFile().delete();
                        }
                    })
                    .command("java", "-cp", classpath, "-Dcassandra.config=" + cassandraConfig.toString(), "-Dcassandra.jmx.local.port=7199", "-Dcassandra.logdir="+logDirectory, "-Dlogback.configurationFile="+logback, className)
                    .start().getFuture();


        LocalDateTime timeout = LocalDateTime.now().plusSeconds(60);

        while (LocalDateTime.now().isBefore(timeout) && !result.isDone() && stderr.toString().isEmpty()) {
            System.out.print(".");
            System.out.flush();

            String output = null;
            try {
                output = new ProcessExecutor().command("java", "-cp", classpath, "-Dlogback.configurationFile="+logback.toString(), org.apache.cassandra.tools.NodeTool.class.getCanonicalName(), "statusthrift")
                        .readOutput(true).execute()
                        .outputUTF8();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }


            if (output.trim().equals("running")) {
                System.out.println("SUCCESS");
                return;
            }

            try {
                Thread.sleep(WAIT_INTERVAL_SECOND * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        System.out.println(stderr.toString());

    }
}
