package ai.grakn.bootup.graknengine.grakn_pid;

public class GraknPidManager {
    GraknPidStore graknPidStore;
    GraknPidRetriever graknPidRetriever;

    public GraknPidManager(GraknPidStore graknPidStore, GraknPidRetriever graknPidRetriever) {
        this.graknPidStore = graknPidStore;
        this.graknPidRetriever = graknPidRetriever;
    }
    public void trackGraknPid() {
        long pid = graknPidRetriever.getPid();
        graknPidStore.trackGraknPid(pid);
    }
}
