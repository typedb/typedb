#!groovy

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

def slackGithub(String message, String color = null) {
    def user = sh(returnStdout: true, script: "git show --format=\"%aN\" | head -n 1").trim()
    String author = "authored by - ${user}"
    String link = "(<${env.BUILD_URL}|Open>)"
    String statusHeader = "${message} on ${env.BRANCH_NAME}: ${env.JOB_NAME} #${env.BUILD_NUMBER}"
    String formatted = "${statusHeader} ${link}\n${author}"
    slackSend channel: "#github", color: color, message: formatted
}

echo 'Terminating existing running builds (only for non master / stable branch)...'
if (!(env.BRANCH_NAME in ['master', 'stable'])) {
    stopAllRunningBuildsForThisJob()
}

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh "mvn --batch-mode install -T 2.5C -DskipTests=true"
            }
        }
//        stage('Unit And Integration Test') {
//            steps {
//                sh "mvn --batch-mode verify"
//                mvn clean verify -P janus -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT -DMaven.test.failure.ignore=true -Dsurefire.rerunFailingTestsCount=1
//            }
//        }
//        stage('SNB End-to-end Test') {
//            steps {
//                PATH=$PATH:./grakn-test/test-snb/src/main/bash:./grakn-test/test-integration/src/main/bash PACKAGE=grakn-package WORKSPACE=`pwd` ./grakn-test/test-snb/src/main/bash/load.sh
//                sh "cd ./grakn-test/test-integration/src/test/bash && ./init-grakn.sh ${env.BRANCH_NAME}"
//                sh "cd ./grakn-test/test-snb/src/main/bash && ./load.sh"
//                sh "cd ./grakn-test/test-snb/src/main/bash && ./validate.sh"
//                sh "cd ./grakn-test/test-integration/src/test/bash && ./stop-grakn.sh"
//            }
//        }
//        stage('SNB End-to-end Test') {
//            steps {
//                sh "cd grakn-dist/target && tar -xf grakn-dist-1.3.0-SNAPSHOT.tar.gz"
//                sh "cd grakn-dist/target/grakn-dist-1.3.0-SNAPSHOT/ && ./grakn server start"
//
//                sh "PATH=$PATH:./grakn-test/test-snb/src/main/bash:./grakn-test/test-integration/src/test/bash PACKAGE=grakn-package WORKSPACE=`pwd` ./grakn-test/test-snb/src/main/bash/load.sh"
//                sh ""
//
//                sh "cd grakn-dist/target/grakn-dist-1.3.0-SNAPSHOT/ && ./grakn server stop"
//                sh "cd grakn-dist/target/ && rm -r grakn-dist-1.3.0-SNAPSHOT"
//            }
//        }
        stage('Biomed End-to-end Test') {
            steps {
                sh "cd grakn-dist/target && tar -xf grakn-dist-1.3.0-SNAPSHOT.tar.gz"
                sh "cd grakn-dist/target/grakn-dist-1.3.0-SNAPSHOT/ && ./grakn server start"

                sh 'PATH=$PATH:./grakn-dist/target/grakn-dist-1.3.0-SNAPSHOT ./grakn-test/test-biomed/load.sh'
                sh 'PATH=$PATH:./grakn-dist/target/grakn-dist-1.3.0-SNAPSHOT ./grakn-test/test-biomed/validate.sh'

                sh "cd grakn-dist/target/grakn-dist-1.3.0-SNAPSHOT/ && ./grakn server stop"
                sh "cd grakn-dist/target/ && rm -r grakn-dist-1.3.0-SNAPSHOT"
            }
        }

    }

    post {
        success {
            slackGithub "Build Success", "good"
        }
        unstable {
            slackGithub "Build Unstable", "danger"
        }
        failure {
            slackGithub "Build Failure", "danger"
        }
        always {
            junit '**/TEST*.xml'
        }
    }
}