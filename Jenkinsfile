#!groovy

// In order to add a new integration test, create a new sub-folder under `grakn-test` with two executable scripts,
// `load.sh` and `validate.sh`. Add the name of the folder to the list `integrationTests` below.
// `validate.sh` will be passed the branch name (e.g. "master") as the first argument
def integrationTests = ["test-snb", "test-biomed"]

//This sets properties in the Jenkins server. In this case run every 8 hours
properties([pipelineTriggers([cron('H H/8 * * *')])])

def slackGithub(String message, String color = null) {
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
    slackSend channel: "#github", color: color, message: """
${message} on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)
authored by - ${user}"""
}

def runIntegrationTest(String moduleName) {
    String modulePath = "${workspace}/grakn-test/${moduleName}"

    stage(moduleName) {
        withEnv(["PATH+EXTRA=${modulePath}:${modulePath}/src/main/bash"]) {
            withGrakn {
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

def withGrakn(Closure closure) {
    withEnv(["PATH+EXTRA=${workspace}/grakn-test/test-integration/src/test/bash"]) {
        //Everything is wrapped in a try catch so we can handle any test failures
        //If one test fails then all the others will stop. I.e. we fail fast
        try {
            timeout(15) {
                //Stages allow you to organise and group things within Jenkins
                stage('Start Grakn') {
                    checkout scm

                    sh "build-grakn.sh ${env.BRANCH_NAME}"

                    archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"

                    sh 'init-grakn.sh'
                }
            }
            closure()
        } catch (error) {
            slackGithub "Periodic Build Failed" "danger"
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

node {
    //Only run validation master/stable
    if (env.BRANCH_NAME in ['master', 'stable'] || true) {
        slackGithub "Build started"

        stage('Run the benchmarks') {
            sh 'mvn clean test  -P janus -Dtest=*Benchmark -DfailIfNoTests=false -Dgrakn.test-profile=janus -Dmaven.repo.local=' + workspace + '/maven -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true'
        }

        for (String moduleName : integrationTests) {
            runIntegrationTest(moduleName)
        }
        slackGithub "Periodic Build Success" "good"
    }
}
