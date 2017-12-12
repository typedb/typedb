package ai.grakn.bootup.graknengine.grakn_pid;

import java.nio.file.Path;

public class GraknPidManagerFactory {
    public static GraknPidManager newGraknPidManagerForUnixOS(Path pidfilePath) {
        GraknPidStore graknPidStore = new GraknPidFileStore(pidfilePath);
        GraknPidRetriever graknPidRetriever = new UnixGraknPidRetriever();
        GraknPidManager graknPidManager = new GraknPidManager(graknPidStore, graknPidRetriever);
        return graknPidManager;
    }
}
