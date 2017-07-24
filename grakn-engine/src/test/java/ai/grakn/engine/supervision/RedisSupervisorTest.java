package ai.grakn.engine.supervision;

import ai.grakn.engine.externalcomponents.ExternalComponentException;
import ai.grakn.engine.externalcomponents.OperatingSystemCalls;
import ai.grakn.engine.externalcomponents.RedisSupervisor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class RedisSupervisorTest {
    @Test
    public void redisStartShouldWorkProperly() throws IOException, InterruptedException {
        final int SUCCESS_EXIT_CODE = 0;
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);

        doReturn(SUCCESS_EXIT_CODE).when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).start();

        // it should execute successfully
        try {
            redisSupervisorMockitoSpy.start();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void redisStartShouldThrowIfExecReturnsNonZero() throws IOException, InterruptedException {
        final int NON_ZERO_EXIT_CODE = 1;
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);

        doReturn(NON_ZERO_EXIT_CODE).when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).start();

        // if should throw an ExternalComponentException
        try {
            redisSupervisorMockitoSpy.start();
            assertTrue(false);
        } catch (ExternalComponentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void redisStopShouldWorkProperly() throws IOException, InterruptedException {
        final int SUCCESS_EXIT_CODE = 0;
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);

        doReturn(0).when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).stop();

        try {
            redisSupervisorMockitoSpy.stop();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void redisStopShouldThrowIfExecReturnsNonZero() throws IOException, InterruptedException {
        final int NON_ZERO_EXIT_CODE = 1;
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);

        doReturn(NON_ZERO_EXIT_CODE).when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).stop();

        try {
            redisSupervisorMockitoSpy.stop();
            assertTrue(false);
        } catch (ExternalComponentException e) {
            assertTrue(true);
        }
    }
}
