package ai.grakn.engine.supervision;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.externalcomponents.CassandraSupervisor;
import ai.grakn.engine.externalcomponents.OperatingSystemCalls;
import ai.grakn.exception.GraknBackendException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class CassandraSupervisorTest {
    @Test
    public void cassandraStartShouldWorkProperly() throws IOException, InterruptedException {
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doNothing().when(osCallsMockitoSpy).execAndReturn(any());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStarted();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).startAsync();

        // it should execute successfully
        try {
            cassandraSupervisorMockitoSpy.startAsync();
        } catch (IOException | InterruptedException e) {
            assertTrue(false);
        }
    }

    @Test
    public void cassandraStartShouldThrowIfExecReturnsNonZero() throws IOException, InterruptedException {
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        Exception t = GraknBackendException.operatingSystemCallException("", 1);
        doThrow(t).when(osCallsMockitoSpy).execAndReturn(any());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStarted();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).startAsync();

        // if should throw an ExternalComponentException
        try {
            cassandraSupervisorMockitoSpy.startAsync();
            assertTrue(false);
        } catch (GraknBackendException e) {
            assertTrue(true);
        }
    }

    @Test
    public void cassandraStopShouldWorkProperly() throws IOException, InterruptedException {
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doReturn(true).when(osCallsMockitoSpy).fileExists(anyString());
        doReturn(-1).when(osCallsMockitoSpy).catPidFile(anyString());
        doNothing().when(osCallsMockitoSpy).execAndReturn(any());
        doReturn(0).when(osCallsMockitoSpy).psP(anyInt());
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
        GraknEngineConfig graknEngineConfig = EngineTestHelper.config();
        OperatingSystemCalls osCallsMockitoSpy = spy(OperatingSystemCalls.class);
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(graknEngineConfig, osCallsMockitoSpy, "");
        CassandraSupervisor cassandraSupervisorMockitoSpy = spy(cassandraSupervisor);

        doReturn(true).when(osCallsMockitoSpy).fileExists(anyString());
        doReturn(-1).when(osCallsMockitoSpy).catPidFile(anyString());
        Exception t = GraknBackendException.operatingSystemCallException("", 1);
        doThrow(t).when(osCallsMockitoSpy).execAndReturn(any());
        doReturn(0).when(osCallsMockitoSpy).psP(anyInt());
        doNothing().when(cassandraSupervisorMockitoSpy).waitForCassandraStopped();
        doCallRealMethod().when(cassandraSupervisorMockitoSpy).stop();

        try {
            cassandraSupervisorMockitoSpy.stop();
            assertTrue(false);
        } catch (GraknBackendException e) {
            assertTrue(true);
        }
    }
}
