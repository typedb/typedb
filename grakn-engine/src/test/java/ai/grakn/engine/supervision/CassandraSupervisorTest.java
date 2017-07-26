package ai.grakn.engine.supervision;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.externalcomponents.CassandraSupervisor;
import ai.grakn.engine.externalcomponents.ExternalComponentException;
import ai.grakn.engine.externalcomponents.OperatingSystemCalls;
import ai.grakn.engine.externalcomponents.RedisSupervisor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CassandraSupervisorTest {
    @Test
    public void cassandraStartShouldWorkProperly() throws IOException, InterruptedException {
        final int SUCCESS_EXIT_CODE = 0;
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doReturn(SUCCESS_EXIT_CODE).when(osCallsMockitoSpy).execAndReturn(any());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStarted();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).start();

        // it should execute successfully
        try {
            cassandraSupervisorMockitoSpy.start();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void cassandraStartShouldThrowIfExecReturnsNonZero() throws IOException, InterruptedException {
        final int NON_ZERO_EXIT_CODE = 1;
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doReturn(NON_ZERO_EXIT_CODE).when(osCallsMockitoSpy).execAndReturn(any());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStarted();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).start();

        // if should throw an ExternalComponentException
        try {
            cassandraSupervisorMockitoSpy.start();
            assertTrue(false);
        } catch (ExternalComponentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void cassandraStopShouldWorkProperly() throws IOException, InterruptedException {
        final int SUCCESS_EXIT_CODE = 0;
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doReturn(true).when(osCallsMockitoSpy).fileExists(anyString());
        doReturn(-1).when(osCallsMockitoSpy).catPidFile(anyString());
        doReturn(0).when(osCallsMockitoSpy).execAndReturn(any());
        doReturn(SUCCESS_EXIT_CODE).when(osCallsMockitoSpy).psP(anyInt());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStopped();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).stop();

        try {
            cassandraSupervisorMockitoSpy.stop();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void cassandraStopShouldThrowIfExecReturnsNonZero() throws IOException, InterruptedException {
        final int NON_ZERO_EXIT_CODE = 1;
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doReturn(true).when(osCallsMockitoSpy).fileExists(anyString());
        doReturn(-1).when(osCallsMockitoSpy).catPidFile(anyString());
        doReturn(NON_ZERO_EXIT_CODE).when(osCallsMockitoSpy).execAndReturn(any());
        doReturn(0).when(osCallsMockitoSpy).psP(anyInt());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStopped();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).stop();

        try {
            cassandraSupervisorMockitoSpy.stop();
            assertTrue(false);
        } catch (ExternalComponentException e) {
            assertTrue(true);
        }
    }
}
