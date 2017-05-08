/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.test.listener;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * <p>
 * A junit listener around each executed test that collects the time it takes to run the test and
 * logs into a file. The file name where time is logged can be specified in a
 * <code>grakn.test.timerecord.file</code> system property. 
 * </p>
 * 
 * @author borislav
 *
 */
public class TimingListener extends RunListener {
    
    private long startTime = 0;
    private String test = "";
    
    private void writeTime(String test, long time) {
        String timeLogFile = System.getProperty("grakn.test.timerecord.file");
        if (timeLogFile == null) {
            timeLogFile = "grakn-test-timings.log";
        }
        try {
            Files.write(Paths.get(timeLogFile), Collections.singleton(test + "," + time), APPEND, CREATE);
            System.out.println("WROTE to file " + Paths.get(timeLogFile));
        } catch (IOException e) {
            System.err.println("Failed to write test time to file " + timeLogFile);
            System.err.println("Please fix configuration or disable test time measurement altogether.");
            e.printStackTrace();
        }
    }
    
    public void testStarted(Description description) throws Exception {
        if (!description.isTest()) {
            return;
        }
        startTime = System.currentTimeMillis();
        test = description.getClassName() + "." + description.getMethodName();
    }

    public void testFinished(Description description) throws Exception {        
        String finishedTest = description.getClassName() + "." + description.getMethodName();
        if (!test.equals(finishedTest)) {
            throw new RuntimeException("Test started " + test + " different from test finished " + finishedTest);
        }
        writeTime(test, (System.currentTimeMillis() - startTime));
    }
}