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

//Only run validation master/stable
if (env.BRANCH_NAME in ['master', 'stable'] || true) {
    properties([buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '7'))])

    node {
        slackGithub "Build started"

        String workspace = pwd()
        checkout scm

        stage('Build Grakn') {
            withScripts(workspace) {
                buildGrakn()
            }

            archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"

            stash includes: 'grakn-dist/target/grakn-dist*.tar.gz', name: 'dist'
        }
    }

    parallel benchmarks: {
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
    },
    integration: {
        node {
            String workspace = pwd()
            checkout scm
            unstash 'dist'

            for (String moduleName : integrationTests) {
                runIntegrationTest(workspace, moduleName)
            }
        }
    },
    longRunning: {
        // Deploy long-running instance on stable branch
        if (env.BRANCH_NAME == 'stable') {
            node {
                checkout scm
                unstash 'dist'

        stage('Deploy Grakn') {
            sshagent(credentials: ['jenkins-aws-ssh']) {
                sh "scp -o StrictHostKeyChecking=no grakn-dist/target/grakn-dist*.tar.gz ubuntu@${LONG_RUNNING_INSTANCE_ADDRESS}:~/"
                sh "scp -o StrictHostKeyChecking=no scripts/repeat-query ubuntu@${LONG_RUNNING_INSTANCE_ADDRESS}:~/"ssh "'bash -s' < scripts/start-long-running-instance.sh"}
                }
            }
        }
    }

    node {
        slackGithub "Periodic Build Success", "good"
    }
}
