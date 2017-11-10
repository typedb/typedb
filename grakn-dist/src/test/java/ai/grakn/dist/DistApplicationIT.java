package ai.grakn.dist;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@Ignore
public class DistApplicationIT {

    @Test
    public void server_start() {
        DistApplication.main(new String[]{"server","start"});
        DistApplication.main(new String[]{"server","start","grakn"});
        DistApplication.main(new String[]{"server","start","queue"});
        DistApplication.main(new String[]{"server","start","storage"});
    }
    @Test
    public void server_stop() {
        DistApplication.main(new String[]{"server","stop"});
        DistApplication.main(new String[]{"server","stop","grakn"});
        DistApplication.main(new String[]{"server","stop","queue"});
        DistApplication.main(new String[]{"server","stop","storage"});
    }
    @Test
    public void server_status() {
        DistApplication.main(new String[]{"server","status"});
    }
    @Test
    public void server_clean() {
        DistApplication.main(new String[]{"server","clean"});
    }
    @Test
    public void server_help() {
        DistApplication.main(new String[]{"server","help"});
    }

    @Test
    public void noArguments() {
        DistApplication.main(new String[]{});
    }

    @PrepareForTest({System.class,DistApplication.class})
    @Test(expected = RuntimeException.class)
    public void version() {
        PowerMockito.mockStatic(System.class);
        Mockito.doThrow(RuntimeException.class).when(System.class);

        DistApplication.main(new String[]{"version"});
    }

    @Test
    public void help() {
        DistApplication.main(new String[]{"help"});
    }

}
