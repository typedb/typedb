package ai.grakn.engine.bootup;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageBootupTest {
    @Test
    public void test() {
//    Path graknHome = Paths.get("C:", "Users", "Grakn Labs", "Desktop", "grakn-core-1.4.0-SNAPSHOT");
//    Path graknProperties = graknHome.resolve("conf").resolve("grakn.properties");
//            new StorageBootup(new BootupProcessExecutor(), graknHome, graknProperties).start();
        Path STORAGE_PIDFILE = Paths.get(System.getProperty("java.io.tmpdir"), "grakn-storage.pid");
        BootupProcessExecutor inst = new BootupProcessExecutor();
        boolean whoknows = inst.isProcessRunning(STORAGE_PIDFILE);
        System.out.println(whoknows);
    }
}
