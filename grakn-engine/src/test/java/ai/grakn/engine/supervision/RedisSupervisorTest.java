package ai.grakn.engine.supervision;

import ai.grakn.engine.externalcomponents.OperatingSystemCalls;
import ai.grakn.engine.externalcomponents.RedisSupervisor;
import ai.grakn.exception.GraknBackendException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RedisSupervisorTest {
    @Test
    public void redisStartShouldWorkProperly() throws IOException, InterruptedException {
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);

        doNothing().when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).start();

        // it should execute successfully
        try {
            redisSupervisorMockitoSpy.start();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void redisStartShouldThrowIfExecThrows() throws IOException, InterruptedException {
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);
        final int NON_ZERO_EXIT_CODE = 1;
        Exception t = GraknBackendException.operatingSystemCallException("", NON_ZERO_EXIT_CODE);
        doThrow(t).when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).start();

        // if should throw an ExternalComponentException
        try {
            redisSupervisorMockitoSpy.start();
            assertTrue(false);
        } catch (GraknBackendException e) {
            assertTrue(true);
        }
    }

    @Test
    public void redisStopShouldWorkProperly() throws IOException, InterruptedException {
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);

        doNothing().when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).stop();

        try {
            redisSupervisorMockitoSpy.stop();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void redisStopShouldThrowIfExecReturnsNonZero() throws IOException, InterruptedException {
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCallsMockitoSpy, "");
        RedisSupervisor redisSupervisorMockitoSpy = spy(redisSupervisor);
        final int NON_ZERO_EXIT_CODE = 1;
        Exception t = GraknBackendException.operatingSystemCallException("", NON_ZERO_EXIT_CODE);
        doThrow(t).when(osCallsMockitoSpy).execAndReturn(any());
        doCallRealMethod().when(redisSupervisorMockitoSpy).stop();

        try {
            redisSupervisorMockitoSpy.stop();
            assertTrue(false);
        } catch (GraknBackendException e) {
            assertTrue(true);
        }
    }
}
