package ai.grakn.engine.supervision;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class GraknComponentSupervisionTest {
//  @Test
//  public void shouldNotStartCassandraIfAlreadyRunning() {
//
//  }
//
//  @Test
//  public void shouldStartCassandraIfNoProcessFound() {
//
//  }
//
//  @Test
//  public void shouldStopCassandraIfRunning() {
//
//  }
//
//  @Test
//  public void shouldStartCassandraIfBinaryFound() {
//    // TODO
//  }
//
//  @Test
//  public void shouldThrowIfBinaryNotFound() {
//    // TODO
//  }
//
//  @Test
//  public void shouldStopCassandra() {
//    // TODO
//  }

  @Test
  public void shouldReturnTrueIfCassandraIsRunning() throws MalformedPidFileException, IOException, InterruptedException {
    final int SUCCESS_EXIT_CODE = 0;

    OperatingSystemCalls osCalls = spy(OperatingSystemCalls.class);
    GraknComponentSupervisor supervision = new GraknComponentSupervisor(osCalls);

    doReturn(true).when(osCalls).fileExists(anyString());
    doReturn(-1).when(osCalls).catPidFile(anyString());
    doReturn(SUCCESS_EXIT_CODE).when(osCalls).psP(anyInt()); // mock a successful ps -p call

    assertEquals(supervision.isCassandraRunning(), true);
  }

  @Test
  public void shouldReturnFalseIfCassandraIsNotRunning() throws IOException, InterruptedException, MalformedPidFileException {
    OperatingSystemCalls osCalls = spy(OperatingSystemCalls.class);
    GraknComponentSupervisor supervision = new GraknComponentSupervisor(osCalls);

    doReturn(false).when(osCalls).fileExists(anyString()); // simulate file not found
    doReturn(0).when(osCalls).psP(anyInt());

    assertEquals(supervision.isCassandraRunning(), false);
  }

  @Test
  public void shouldThrowIfPsExitCodeIsNonZero() throws IOException, InterruptedException, MalformedPidFileException {
    final int NON_ZERO_EXIT_CODE = 1;
    OperatingSystemCalls osCalls = spy(OperatingSystemCalls.class);
    GraknComponentSupervisor supervision = new GraknComponentSupervisor(osCalls);

    doReturn(true).when(osCalls).fileExists(anyString());
    doReturn(-1).when(osCalls).catPidFile(anyString());
    doReturn(NON_ZERO_EXIT_CODE).when(osCalls).psP(anyInt()); // mock an unsuccessful ps -p call (i.e., due to incorrect PID)

    try {
      supervision.isCassandraRunning();
      assertEquals(true, false); // the above code is expected to throw, thus deliberately return false here
    } catch (RuntimeException e) {
      // it throws as expected
    }
  }
}
