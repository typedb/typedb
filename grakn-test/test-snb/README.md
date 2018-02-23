# Description
The `test-snb` project perform tests with the [SNB data](ldbcouncil.org/developer/snb). In principle, there are three major steps involved:
1. Generating an initial dataset (with the help of the SNB LDBC Datagen)
2. Loading the schema and the generated data into Grakn (with the help of `graql migrate`)
3. Validating if the data has been inserted correctly (done with the `GraknDB` client implementation located under `grakn-test/test-snb/src/main/java/ai/grakn/GraknDB.java`)

## Prerequisites
### Hadoop
1. Download Hadoop (http://hadoop.apache.org/releases.html): `wget http://mirrors.ukfast.co.uk/sites/ftp.apache.org/hadoop/common/hadoop-2.9.0/hadoop-2.9.0.tar.gz`
2. Extract it: `tar -xf hadoop-2.9.0.tar.gz`
3. Add an environment variable pointing to where it was extracted: `export HADOOP_HOME=/path/to/hadoop`

### SNB Data generator
1. Clone the [SNB LDBC Datagen](https://github.com/ldbc/ldbc_snb_datagen) repository: `git clone https://github.com/ldbc/ldbc_snb_datagen`
2. Add an environment variable pointing to where it was extracted: `export LDBC_SNB_DATAGEN_HOME=/path/to/snb-ldbc-datagen`
3. Compile and generate a fat-JAR using Maven: `cd /path/to/snb-ldbc-datagen && mvn clean compile assembly:single`

## Running The Validation
### With The Validation Dataset
The following steps outlines generating, loading, and validating data with the "validation dataset".
1. If you want to do everything at once, i.e. generate data, loading, and validating: `cd $GRAKN_HOME && ./scripts/jenkins.sh test-snb`
2. If you want to load the validation dataset only: `cd $GRAKN_HOME && ./scripts/load.sh test-snb`
3. If you want to perform only the validation step on the validation dataset: `cd $GRAKN_HOME && ./scripts/validate.sh test-snb`

### With The SF* Dataset
The following steps outlines generating the `SF*` dataset.
1. `cd $GRAKN_HOME && ./scripts/load.sh test-snb gen SF1`

Note that currently it is not possible to perform validation against it.

## The Output Of A Successful Verification
A successful verification would look like the following:
```
----essed 6,987 / 11,277 -- Crashed 0 -- Incorrect 0 -- Currently processing LdbcShortQuery1PersonProfile...
21:27:55.354 [com.ldbc.driver.Client.main()] DEBUG ai.grakn.factory.EmbeddedGraknSession - Response from engine []
[WARNING] thread Thread[pool-6-thread-3,5,com.ldbc.driver.Client] was interrupted but is still alive after waiting at least 15000msecs
[WARNING] thread Thread[pool-6-thread-3,5,com.ldbc.driver.Client] will linger despite being asked to die via interruption
[WARNING] thread Thread[pool-6-thread-4,5,com.ldbc.driver.Client] will linger despite being asked to die via interruption
[WARNING] thread Thread[pool-6-thread-5,5,com.ldbc.driver.Client] will linger despite being asked to die via interruption
[WARNING] NOTE: 3 thread(s) did not finish despite being asked to  via interruption. This is not a problem with exec:java, it is a problem with the running code. Although not serious, it should be remedied.
[WARNING] Couldn't destroy threadgroup org.codehaus.mojo.exec.ExecJavaMojo$IsolatedThreadGroup[name=com.ldbc.driver.Client,maxpri=10]
java.lang.IllegalThreadStateException
	at java.lang.ThreadGroup.destroy(ThreadGroup.java:778)
	at org.codehaus.mojo.exec.ExecJavaMojo.execute(ExecJavaMojo.java:328)
	at org.apache.maven.plugin.DefaultBuildPluginManager.executeMojo(DefaultBuildPluginManager.java:134)
	at org.apache.maven.lifecycle.internal.MojoExecutor.execute(MojoExecutor.java:208)
	at org.apache.maven.lifecycle.internal.MojoExecutor.execute(MojoExecutor.java:154)
	at org.apache.maven.lifecycle.internal.MojoExecutor.execute(MojoExecutor.java:146)
	at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject(LifecycleModuleBuilder.java:117)
	at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject(LifecycleModuleBuilder.java:81)
	at org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder.build(SingleThreadedBuilder.java:51)
	at org.apache.maven.lifecycle.internal.LifecycleStarter.execute(LifecycleStarter.java:128)
	at org.apache.maven.DefaultMaven.doExecute(DefaultMaven.java:309)
	at org.apache.maven.DefaultMaven.doExecute(DefaultMaven.java:194)
	at org.apache.maven.DefaultMaven.execute(DefaultMaven.java:107)
	at org.apache.maven.cli.MavenCli.execute(MavenCli.java:993)
	at org.apache.maven.cli.MavenCli.doMain(MavenCli.java:345)
	at org.apache.maven.cli.MavenCli.main(MavenCli.java:191)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.codehaus.plexus.classworlds.launcher.Launcher.launchEnhanced(Launcher.java:289)
	at org.codehaus.plexus.classworlds.launcher.Launcher.launch(Launcher.java:229)
	at org.codehaus.plexus.classworlds.launcher.Launcher.mainWithExitCode(Launcher.java:415)
	at org.codehaus.plexus.classworlds.launcher.Launcher.main(Launcher.java:356)
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 31:28 min
[INFO] Finished at: 2018-01-08T21:28:10Z
[INFO] Final Memory: 126M/1750M
[INFO] ------------------------------------------------------------------------
++ cat $GRAKN_HOME/social_network/validation_params-failed-actual.json
+ FAILURES='[ ]'
++ cat $GRAKN_HOME/social_network/validation_params-failed-expected.json
+ EXPECTED='[ ]'
+ '[' '[ ]' == '[ ]' ']'
+ echo 'Validation completed without failures.'
Validation completed without failures.
```

### Explanation
1. Pay attention to the fact that there are 0 verification marked as Crashed or Incorrect by looking at the line which looks like:
```
----essed 6,987 / 11,277 -- Crashed 0 -- Incorrect 0 -- Currently processing LdbcShortQuery1PersonProfile...
```

2. The variable `FAILURES` is empty:
```
FAILURES='[ ]'
```
If that isn't the case, have a look at the file `social_network/validation_params-failed-actual.json` and `social_network/validation_params-failed-expected.json` which record the erroneous result(s).

### Additional Remarks
1. We are only processing 6987 out of the overall 11277 elements which need to be verified, as shown on the line which starts with `----essed 6,987 / 11,277`. This is normal, as some tests are currently ignored for being too slow. In the future we would ideally need to enable everyhing.
2. The `java.lang.IllegalThreadStateException` happening at the end of test can be ignored (for now), as it is only a minor cleanup issue.
