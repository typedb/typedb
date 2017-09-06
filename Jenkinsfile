#!groovy
//This sets properties in the Jenkins server. In this case run every 8 hours
properties([pipelineTriggers([cron('H H/8 * * *')])])

def slackGithub = { String message, String color = null ->
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
    slackSend channel: "#github", color: color, message: """
${message} on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)
authored by - ${user}"""
}

def buildGrakn = {
    checkout scm
    slackGithub "Build started"

    sh "build-grakn.sh ${env.BRACH_NAME}"

    archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"
}

def initGrakn = {
    sh 'init-grakn.sh'
}

def testConnection = {
    sh 'test-connection.sh'
}

def loadValidationData = {
    sh '../grakn-test/test-snb/src/generate-SNB/download-SNB.sh'
    sh '../grakn-test/test-snb/src/generate-SNB/load-SNB.sh arch validate'
}

def measureSize = {
    sh 'measure-size.sh'
}

def buildSnbConnectors = {
    sh "../grakn-test/test-snb/src/generate-SNB/build-snb-connectors.sh"
}

def validateQueries = {
    sh '../grakn-test/test-snb/src/validate-snb/validate.sh'
}

def tearDownGrakn = {
    archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
    sh 'tear-down.sh'
}

node {
    withEnv(["PATH+EXTRA=${workspace}/grakn-test/test-integration/src/test/bash"]) {
        //Everything is wrapped in a try catch so we can handle any test failures
        //If one test fails then all the others will stop. I.e. we fail fast
        try {
            def workspace = pwd()
            //Always wrap each test block in a timeout
            //This first block sets up engine within 15 minutes
            timeout(15) {
                //Stages allow you to organise and group things within Jenkins
                stage('Build Grakn', buildGrakn)
                stage('Init Grakn', initGrakn)
                stage('Test Connection', testConnection)
            }
            //Only run validation master/stable
            if (env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'stable' || true) {
                //Sets up environmental variables which can be shared between multiple tests
                withEnv(["LDBC_DRIVER=${workspace}/.m2/repository/com/ldbc/driver/jeeves/0.3-SNAPSHOT/jeeves-0.3-SNAPSHOT.jar",
                         "LDBC_CONNECTOR=${workspace}/grakn-test/test-snb/target/test-snb-${env.BRANCH_NAME}-jar-with-dependencies.jar",
                         "LDBC_VALIDATION_CONFIG=${workspace}/grakn-test/test-snb/src/validate-snb/readwrite_grakn--ldbc_driver_config--db_validation.properties"]) {
                    timeout(180) {
                        dir('generate-SNB') {
                            stage('Load Validation Data', loadValidationData)
                        }
                        stage('Measure Size', measureSize)
                    }
                    timeout(360) {
                        dir('grakn-test/test-snb/') {
                            stage('Build the SNB connectors', buildSnbConnectors)
                        }
                        dir('validate-snb') {
                            stage('Validate Queries', validateQueries)
                        }
                    }
                }
                slackGithub "Periodic Build Success" "good"
            }
        } catch (error) {
            slackGithub "Periodic Build Failed" "danger"
            throw error
        } finally { // Tears down test environment
            timeout(5) {
                stage('Tear Down Grakn', tearDownGrakn)
            }
        }
    }
}
