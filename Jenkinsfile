#!groovy

import static Constants.*;

// In order to add a new integration test, create a new sub-folder under `grakn-test` with two executable scripts,
// `load.sh` and `validate.sh`. Add the name of the folder to the list `integrationTests` below.
// `validate.sh` will be passed the branch name (e.g. "master") as the first argument
def integrationTests = ["test-snb"]

class Constants {
    static final LONG_RUNNING_INSTANCE_ADDRESS = '172.31.22.83'
}

//This sets properties in the Jenkins server. In this case run every 8 hours
properties([pipelineTriggers([cron('H H/8 * * *')])])
properties([buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '7'))])

def slackGithub(String message, String color = null) {
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()

    String author = "authored by - ${user}"
    String link = "(<${env.BUILD_URL}|Open>)"
    String branch = env.BRANCH_NAME;

    String formattedMessage = "${message} on ${branch}: ${env.JOB_NAME} #${env.BUILD_NUMBER} ${link}\n${author}"

    slackSend channel: "#github", color: color, message: formattedMessage
}

def runIntegrationTest(String workspace, String moduleName) {
    String modulePath = "${workspace}/grakn-test/${moduleName}"

    stage(moduleName) {
        withPath("${modulePath}:${modulePath}/src/main/bash") {
            withGrakn(workspace) {
                timeout(180) {
                    stage('Load') {
                        sh "load.sh"
                    }
                }
                timeout(360) {
                    stage('Validate') {
                        sh "validate.sh ${env.BRANCH_NAME}"
                    }
                }
            }
        }
    }
}

def withGrakn(String workspace, Closure closure) {
    withScripts(workspace) {
        //Everything is wrapped in a try catch so we can handle any test failures
        //If one test fails then all the others will stop. I.e. we fail fast
        try {
            timeout(15) {
                //Stages allow you to organise and group things within Jenkins
                stage('Start Grakn') {
                    sh 'init-grakn.sh'
                }
            }
            closure()
        } catch (error) {
            slackGithub "Periodic Build Failed", "danger"
            throw error
        } finally { // Tears down test environment
            timeout(5) {
                stage('Stop Grakn') {
                    archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
                    sh 'tear-down.sh'
                }
            }
        }
    }
}

def withPath(String path, Closure closure) {
    return withEnv(["PATH+EXTRA=${path}"], closure)
}


def withScripts(String workspace, Closure closure) {
    withPath("${workspace}/grakn-test/test-integration/src/test/bash", closure)
}

def ssh(String command) {
    sh "ssh -o StrictHostKeyChecking=no -l ubuntu ${LONG_RUNNING_INSTANCE_ADDRESS} ${command}"
}

def buildGrakn() {
    sh "build-grakn.sh ${env.BRANCH_NAME}"
}

//Run all tests
node {
    String workspace = pwd()
    checkout scm

    slackGithub "Janus tests started"

    void runTests(def args) {
      /* Request the test groupings.  Based on previous test results. */
      /* see https://wiki.jenkins-ci.org/display/JENKINS/Parallel+Test+Executor+Plugin and demo on github
      /* Using arbitrary parallelism of 4 and "generateInclusions" feature added in v1.8. */
      def splits = splitTests parallelism: [$class: 'CountDrivenParallelism', size: 4], generateInclusions: true
     
      /* Create dictionary to hold set of parallel test executions. */
      def testGroups = [:]
     
	for (int i = 0; i < splits.size(); i++) {
	def split = splits[i]
     
	/* Loop over each record in splits to prepare the testGroups that we'll run in parallel. */
	/* Split records returned from splitTests contain { includes: boolean, list: List<string>  }. */
	/*     includes = whether list specifies tests to include (true) or tests to exclude (false). */
	/*     list = list of tests for inclusion or exclusion. */
	/* The list of inclusions is constructed based on results gathered from */
	/* the previous successfully completed job. One additional record will exclude */
	/* all known tests to run any tests not seen during the previous run.  */
	testGroups["split-${i}"] = {  // example, "split3"
	  node {
	    checkout scm
     
	    /* Clean each test node to start. */
	    mvn 'clean'
     
	    def mavenVerify = 'verify -P janus -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT -DMaven.test.failure.ignore=true'
     
	    /* Write includesFile or excludesFile for tests.  Split record provided by splitTests. */
	    /* Tell Maven to read the appropriate file. */
	    if (split.includes) {
	      writeFile file: "target/parallel-test-includes-${i}.txt", text: split.list.join("\n")
	      mavenVerify += " -Dsurefire.includesFile=target/parallel-test-includes-${i}.txt"
	    
	    } else {
	      writeFile file: "target/parallel-test-excludes-${i}.txt", text: split.list.join("\n")
	      mavenVerify += " -Dsurefire.excludesFile=target/parallel-test-excludes-${i}.txt"
	    
	    } // if split
     
	    try {
	    /* Call the Maven build with tests. */
	      mvn mavenVerify
     
	    /* Archive the test results */
	    } finally {
		junit "**/TEST*.xml"
	    } //try
	  } // node
	} // testGroups
    } // for

    timeout(120) {
	stage('Run Janus test profile') {
	  parallel testGroups
	}
    } // timeout

  } // void

    slackGithub "Janus tests success", "good"
}

//Only run validation master/stable
if (env.BRANCH_NAME in ['master', 'stable']) {
    node {
        slackGithub "Build started"

        String workspace = pwd()
        checkout scm

        stage('Build Grakn') {
            withScripts(workspace) {
                buildGrakn()
            }

            archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"

            // Stash the built distribution so other nodes can access it
            stash includes: 'grakn-dist/target/grakn-dist*.tar.gz', name: 'dist'
        }
    }

    // This is a map of jobs to perform in parallel, name -> job closure
    jobs = [
        benchmarks: {
            node {
                String workspace = pwd()
                checkout scm
                unstash 'dist'

                timeout(60) {
                    stage('Run the benchmarks') {
                        sh "mvn clean test --batch-mode -P janus -Dtest=*Benchmark -DfailIfNoTests=false -Dmaven.repo.local=${workspace}/maven -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true"
                        archiveArtifacts artifacts: 'grakn-test/test-integration/benchmarks/*.json'
                    }
                }
            }
        }
    ];

    for (String moduleName : integrationTests) {
        // Add each integration test as a parallel job
        jobs[moduleName] = {
            node {
                String workspace = pwd()
                checkout scm
                unstash 'dist'

                runIntegrationTest(workspace, moduleName)
            }
        }
    }

    // Execute all jobs in parallel
    parallel(jobs);

    node {
        // only deploy long-running instance on stable branch if all tests pass
        if (env.BRANCH_NAME == 'stable') {
            checkout scm
            unstash 'dist'

            stage('Deploy Grakn') {
                sshagent(credentials: ['jenkins-aws-ssh']) {
                    sh "scp -o StrictHostKeyChecking=no grakn-dist/target/grakn-dist*.tar.gz ubuntu@${LONG_RUNNING_INSTANCE_ADDRESS}:~/"
                    sh "scp -o StrictHostKeyChecking=no scripts/repeat-query ubuntu@${LONG_RUNNING_INSTANCE_ADDRESS}:~/"
                    ssh "'bash -s' < scripts/start-long-running-instance.sh"
                }
            }
        }

        slackGithub "Periodic Build Success", "good"
    }
}
