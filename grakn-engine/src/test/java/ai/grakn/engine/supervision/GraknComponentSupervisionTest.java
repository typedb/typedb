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
    GraknComponentSupervisor supervisionSpy = spy(GraknComponentSupervisor.class);

    doReturn(true).when(supervisionSpy).fileExists(anyString());
    doReturn(-1).when(supervisionSpy).catPidFile(anyString());
    doReturn(SUCCESS_EXIT_CODE).when(supervisionSpy).psP(anyInt()); // mock a successful ps -p call

    assertEquals(supervisionSpy.isCassandraRunning(), true);
  }

  @Test
  public void shouldReturnFalseIfCassandraIsNotRunning() throws IOException, InterruptedException, MalformedPidFileException {
    GraknComponentSupervisor supervisionSpy = spy(GraknComponentSupervisor.class);

    doReturn(false).when(supervisionSpy).fileExists(anyString()); // simulate file not found
    doReturn(0).when(supervisionSpy).psP(anyInt());

    assertEquals(supervisionSpy.isCassandraRunning(), false);
  }

  @Test
  public void shouldThrowIfPsExitCodeIsNonZero() throws IOException, InterruptedException, MalformedPidFileException {
    final int NON_ZERO_EXIT_CODE = 1;
    GraknComponentSupervisor supervision = spy(GraknComponentSupervisor.class);

    doReturn(true).when(supervision).fileExists(anyString());
    doReturn(-1).when(supervision).catPidFile(anyString());
    doReturn(NON_ZERO_EXIT_CODE).when(supervision).psP(anyInt()); // mock an unsuccessful ps -p call (i.e., due to incorrect PID)

    try {
      supervision.isCassandraRunning();
      assertEquals(true, false); // the above code is expected to throw, thus deliberately return false here
    } catch (RuntimeException e) {
      // it throws as expected
    }
  }
}
