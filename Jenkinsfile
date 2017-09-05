#!groovy
//This sets properties in the Jenkins server. In this case run every 8 hours
properties([pipelineTriggers([cron('H H/8 * * *')])])

def buildGrakn = {
    checkout scm
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
    slackSend channel: "#github", message: """
Build Started on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)
authored by - ${user}"""

    sh "grakn-test/src/test/bash/build-grakn.sh ${workspace} ${env.BRACH_NAME}"

    archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"
}

def initGrakn = {
    sh 'grakn-test/src/test/bash/init-grakn.sh'
}

def testConnection = {
    sh 'grakn-test/src/test/bash/test-connection.sh'
}

def loadValidationData = {
    sh '../grakn-test/test-snb/src/generate-SNB/download-SNB.sh'
    sh '../grakn-test/test-snb/src/generate-SNB/load-SNB.sh arch validate'
}

def measureSize = {
    sh 'grakn-test/src/test/bash/measure-size.sh'
}

def buildSnbConnectors = {
    sh "../grakn-test/test-snb/src/generate-SNB/build-snb-connectors.sh ${workspace}"
}

def validateQueries = {
    sh '../grakn-test/test-snb/src/validate-snb/validate.sh'
}

def tearDownGrakn = {
    archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
    sh 'grakn-test/src/test/bash/tear-down.sh'
}

node {
    //Everything is wrapped in a try catch so we can handle any test failures
    //If one test fails then all the others will stop. I.e. we fail fast
    try {
        def workspace = pwd()
        //Always wrap each test block in a timeout
        //This first block sets up engine within 15 minutes
        withEnv([
                "PATH+EXTRA=${workspace}/grakn-package/bin"
        ]) {
            timeout(15) {
                //Stages allow you to organise and group things within Jenkins
                stage('Build Grakn') buildGrakn
                stage('Init Grakn') initGrakn
                stage('Test Connection') testConnection
            }
        }
        //Only run validation master/stable
        if (env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'stable') {
            //Sets up environmental variables which can be shared between multiple tests
            withEnv(["VALIDATION_DATA=${workspace}/generate-SNB/readwrite_neo4j--validation_set.tar.gz",
                     "CSV_DATA=${workspace}/generate-SNB/social_network",
                     'KEYSPACE=snb',
                     'ENGINE=localhost:4567',
                     'ACTIVE_TASKS=1000',
                     "PATH+EXTRA=${workspace}/grakn-package/bin",
                     "LDBC_DRIVER=${workspace}/.m2/repository/com/ldbc/driver/jeeves/0.3-SNAPSHOT/jeeves-0.3-SNAPSHOT.jar",
                     "LDBC_CONNECTOR=${workspace}/grakn-test/test-snb/target/test-snb-${env.BRANCH_NAME}-jar-with-dependencies.jar",
                     "LDBC_VALIDATION_CONFIG=${workspace}/grakn-test/test-snb/src/validate-snb/readwrite_grakn--ldbc_driver_config--db_validation.properties"]) {
                timeout(180) {
                    dir('generate-SNB') {
                        stage('Load Validation Data') loadValidationData
                    }
                    stage('Measure Size') measureSize
                }
                timeout(360) {
                    dir('grakn-test/test-snb/') {
                        stage('Build the SNB connectors') buildSnbConnectors
                    }
                    dir('validate-snb') {
                        stage('Validate Queries') validateQueries
                    }
                }
            }
            def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
            slackSend channel: "#github", color: "good", message: """
  Periodic Build Success on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)
  authored by - ${user}"""
        }
    } catch (error) {
        def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
        slackSend channel: "#github", color: "danger", message: """
Periodic Build Failed on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)
authored by - ${user}"""
        throw error
    } finally { // Tears down test environment
        timeout(5) {
            stage('Tear Down Grakn') tearDownGrakn
        }
    }
}
