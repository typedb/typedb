#!groovy

import static Constants.*

def isMainBranch() {
    return env.BRANCH_NAME in ['master', 'stable']
}

// Jenkins normally serializes every variable in a Jenkinsfile so it can pause and resume jobs.
// This method contains variables representing 'jobs', which cannot be serialized.
// The `@NonCPS` annotation stops Jenkins trying to serialize the variables in this method.
@NonCPS
def stopAllRunningBuildsForThisJob() {
    def job = Jenkins.instance.getItemByFullName(env.JOB_NAME)

    for (build in job.builds) {
        if (build.isBuilding() && build.getNumber() != env.BUILD_NUMBER.toInteger()) {
            build.doStop()
        }
    }
}

if (!isMainBranch()) {
    stopAllRunningBuildsForThisJob()
}

// In order to add a new integration test, create a new sub-folder under `grakn-test` with two executable scripts,
// `load.sh` and `validate.sh`. Add the name of the folder to the list `integrationTests` below.
def integrationTests = ["test-snb", "test-biomed"]

class Constants {
    static final LONG_RUNNING_INSTANCE_ADDRESS = '172.31.22.83'
}

//This sets properties in the Jenkins server. In this case run every 8 hours
properties([
        pipelineTriggers([
                issueCommentTrigger('.*!ci.*')
        ]),
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '7'))
])

def slackGithub(String message, String color = null) {
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()

    String author = "authored by - ${user}"
    String link = "(<${env.BUILD_URL}|Open>)"
    String branch = env.BRANCH_NAME

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
                        sh "validate.sh"
                    }
                }
            }
        }
    }
}

def withGrakn(String workspace, Closure closure) {
    //Stages allow you to organise and group things within Jenkins
    try {
        timeout(15) {
            stage('Start Grakn') {
                sh 'init-grakn.sh'
            }
        }
        closure()
    } finally {
        archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
        archiveArtifacts artifacts: 'grakn-package/logs/cassandra.log'
    }
}

def graknNode(Closure closure) {
    //Everything is wrapped in a try catch so we can handle any test failures
    //If one test fails then all the others will stop. I.e. we fail fast
    node {
        withScripts(workspace) {
            try {
                closure()
            } catch (error) {
                slackGithub "Build Failure", "danger"
                throw error
            } finally {
                stage('Tear Down') {
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

def shouldRunAllTests() {
    return isMainBranch()
}

def shouldDeployLongRunningInstance() {
    return env.BRANCH_NAME == 'stable'
}

def mvn(String args) {
    sh "mvn ${args}"
}

Closure createTestJob(split, i, testTimeout) {
    return {
        graknNode {
            String workspace = pwd()
            checkout scm

            slackGithub "Janus tests started"

            def mavenVerify = 'clean verify -P janus -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT -DMaven.test.failure.ignore=true'

            /* Write includesFile or excludesFile for tests.  Split record provided by splitTests. */
            /* Tell Maven to read the appropriate file. */
            if (split.includes) {
                writeFile file: "${workspace}/parallel-test-includes-${i}.txt", text: split.list.join("\n")
                mavenVerify += " -Dsurefire.includesFile=${workspace}/parallel-test-includes-${i}.txt"
            } else {
                writeFile file: "${workspace}/parallel-test-excludes-${i}.txt", text: split.list.join("\n")
                mavenVerify += " -Dsurefire.excludesFile=${workspace}/parallel-test-excludes-${i}.txt"

            }

            try {
                /* Call the Maven build with tests. */
                timeout(testTimeout) {
                    stage('Run Janus test profile') {
                        mvn mavenVerify
                    }
                }
            } finally {
                /* Archive the test results */
                junit "**/TEST*.xml"
            }
        }
    }
}

//Add all tests to job map
void addTests(jobs) {
    /* Request the test groupings.  Based on previous test results. */
    /* see https://wiki.jenkins-ci.org/display/JENKINS/Parallel+Test+Executor+Plugin and demo on github
    /* Using arbitrary parallelism of 4 and "generateInclusions" feature added in v1.8. */
    def splits = splitTests parallelism: [$class: 'CountDrivenParallelism', size: 4], generateInclusions: true

    def numSplits = splits.size()

    // change timeout based on how many splits we have. e.g. 1 split = 120min, 3 splits = 60min
    def testTimeout = 30 + 90 / numSplits;

    splits.eachWithIndex { split, i ->
        jobs["split-${i}"] = createTestJob(split, i, testTimeout)
    }
}

// This is a map that we fill with jobs to perform in parallel, name -> job closure
jobs = [:]

addTests(jobs)

if (shouldRunAllTests()) {

    // Build grakn so it can be used by benchmarks and integration tests
    graknNode {
        slackGithub "Build started"

        checkout scm

        stage('Build Grakn') {
            buildGrakn()

            archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"

            // Stash the built distribution so other nodes can access it
            stash includes: 'grakn-dist/target/grakn-dist*.tar.gz', name: 'dist'
        }
    }

    jobs['benchmarks'] = {
        graknNode {
            String workspace = pwd()
            checkout scm
            unstash 'dist'

            timeout(60) {
                stage('Run the benchmarks') {
                    mvn "clean test --batch-mode -P janus -Dtest=*Benchmark -DfailIfNoTests=false -Dmaven.repo.local=${workspace}/maven -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true"
                    archiveArtifacts artifacts: 'grakn-test/test-integration/benchmarks/*.json'
                }
            }
        }
    }

    for (String moduleName : integrationTests) {
        // We have to re-assign so this variable is always the same in the closure
        String mod = moduleName

        // Add each integration test as a parallel job
        jobs[moduleName] = {
            graknNode {
                String workspace = pwd()
                checkout scm
                unstash 'dist'

                runIntegrationTest(workspace, mod)
            }
        }
    }
}

// Execute all jobs in parallel
parallel(jobs)

if (shouldRunAllTests()) {
    graknNode {
        // only deploy long-running instance on stable branch if all tests pass
        if (shouldDeployLongRunningInstance()) {
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

        slackGithub "Build Success", "good"
    }
}
