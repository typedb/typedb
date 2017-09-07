#!groovy
//This sets properties in the Jenkins server. In this case run every 8 hours
properties([pipelineTriggers([cron('H H/8 * * *')])])

def slackGithub(String message, String color=null) {
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
    slackSend channel: "#github", color: color, message: """
${message} on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)
authored by - ${user}"""
}

Closure buildGrakn = {
    checkout scm
    slackGithub "Build started"

    sh "build-grakn.sh ${env.BRACH_NAME}"

    archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"
}

Closure initGrakn = {
    sh 'init-grakn.sh'
}

Closure testConnection = {
    sh 'test-connection.sh'
}

Closure loadValidationData = {
    sh 'download-snb.sh'
    sh 'load-SNB.sh arch validate'
}

Closure measureSize = {
    sh 'measure-size.sh'
}

Closure buildSnbConnectors = {
    sh "build-snb-connectors.sh"
}

Closure validateQueries = {
    sh "validate.sh ${env.BRANCH_NAME}"
}

Closure tearDownGrakn = {
    archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
    sh 'tear-down.sh'
}

node {
    withEnv(["PATH+EXTRA=${workspace}/grakn-test/test-integration/src/test/bash:${workspace}/grakn-test/test-snb/src/main/bash"]) {
        //Everything is wrapped in a try catch so we can handle any test failures
        //If one test fails then all the others will stop. I.e. we fail fast
        try {
            //Always wrap each test block in a timeout
            //This first block sets up engine within 15 minutes
            timeout(15) {
                //Stages allow you to organise and group things within Jenkins
                stage('Build Grakn', buildGrakn)
                stage('Init Grakn', initGrakn)
                stage('Test Connection', testConnection)
            }
            //Only run validation master/stable
            if (env.BRANCH_NAME in ['master', 'stable'] || true) {
                timeout(180) {
                    stage('Load Validation Data', loadValidationData)
                    stage('Measure Size', measureSize)
                }
                timeout(360) {
                    stage('Build the SNB connectors', buildSnbConnectors)
                    stage('Validate Queries', validateQueries)
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
