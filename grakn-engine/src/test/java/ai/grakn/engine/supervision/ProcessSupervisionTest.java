package ai.grakn.engine.supervision;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

public class ProcessSupervisionTest {
  @Test
  public void shouldNotStartCassandraIfAlreadyRunning() {

  }

  @Test
  public void shouldStartCassandraIfNoProcessFound() {

  }

  @Test
  public void shouldStopCassandraIfRunning() {

  }

  @Test
  public void shouldStartCassandraIfBinaryFound() {
    // TODO
  }

  @Test
  public void shouldThrowIfBinaryNotFound() {
    // TODO
  }

  @Test
  public void shouldStopCassandra() {
    // TODO
  }

//  @Test
//  public void shouldReturnTrueSinceCassandraIsRunning() {
//    ProcessSupervision supervision = new ProcessSupervision();
//    ProcessSupervision supervisionSpy = spy(supervision);
//
//    when(supervisionSpy.fileExists(anyString())).thenReturn(true);
//    when(supervisionSpy.catPidFile(anyString())).thenReturn(0);
//    when(supervisionSpy.psP(anyInt())).thenReturn(0);
//
//    assertEquals(supervisionSpy.isCassandraRunning(), true);
//  }

  @Test
  public void shouldReturnFalseSinceCassandraIsNotRunning() {
    ProcessSupervision supervision = new ProcessSupervision();
    ProcessSupervision supervisionSpy = spy(supervision);

    when(supervisionSpy.fileExists(anyString())).thenReturn(false); // simulate file not found
    when(supervisionSpy.psP(anyInt())).thenReturn(0);

    assertEquals(supervisionSpy.isCassandraRunning(), false);
  }

  @Test
  public void shouldThrowIfPsExitCodeIsNonZero() {
    try {
      final int NON_ZERO_EXIT_CODE = 1;
      ProcessSupervision supervision = new ProcessSupervision();
      ProcessSupervision supervisionSpy = spy(supervision);

      when(supervisionSpy.fileExists(anyString())).thenReturn(true);
      when(supervisionSpy.psP(anyInt())).thenReturn(NON_ZERO_EXIT_CODE); // simulate ps -p unsuccessful return value (due to incorrect PID)

      supervisionSpy.isCassandraRunning();

      assertEquals(true, false); // deliberately return false
    } catch (RuntimeException e) {
      assertEquals(true, true); // deliberately return true
    }
  }
}
