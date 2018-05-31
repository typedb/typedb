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

/**
 * Configurations
 */
LONG_RUNNING_INSTANCE_ADDRESS='172.31.22.83'

/**
 * Main
 */
echo 'Terminating existing running builds (only for non master / stable branch)...'
if (!(env.BRANCH_NAME in ['master', 'stable'])) {
    stopAllRunningBuildsForThisJob()
}

pipeline {
    agent any

    stages {
        stage('Build + Configure') {
            steps {
                sh 'mvn versions:set -DnewVersion=test -DgenerateBackupPoms=false'
                sh "mvn --batch-mode install -T 2.5C -DskipTests=true"
            }
        }

        stage('Unit + IT Tests') {
            steps {
                script {
                    def splits = splitTests parallelism: [$class: 'CountDrivenParallelism', size: 4], generateInclusions: true
                    def numSplits = splits.size()
                    def testTimeout = 30 + 90 / numSplits // change timeout based on how many splits we have. e.g. 1 split = 120min, 3 splits = 60min

                    splits.eachWithIndex { split, i ->
                        node {
                            String workspace = pwd()
                            try {
                                checkout scm

                                def mavenVerify = 'clean verify -P janus -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT -DMaven.test.failure.ignore=true -Dsurefire.rerunFailingTestsCount=1'

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
                                            sh 'mvn ' + mavenVerify
                                        }
                                    }
                                } finally {
                                    /* Archive the test results */
                                    junit "**/TEST*.xml"
                                }
                            } finally {
                                stage('Tear Down') {
                                    sh './grakn-test/test-integration/src/test/bash/tear-down.sh'
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('E2E Tests - grakn server start') {
            steps {
                sh "cd grakn-dist/target && tar -xf grakn-dist-test.tar.gz"
                sh "cd grakn-dist/target/grakn-dist-test/ && ./grakn server start"

            }
        }

        stage('E2E Tests - run') {
            parallel {
                stage('SNB') {
                    steps {
                        sh 'PATH=$PATH:./grakn-test/test-snb/src/main/bash:./grakn-test/test-integration/src/test/bash:./grakn-dist/target/grakn-dist-test PACKAGE=./grakn-dist/target/grakn-dist-test WORKSPACE=. ./grakn-test/test-snb/src/main/bash/load.sh'
                        sh 'PATH=$PATH:./grakn-test/test-snb/src/main/bash:./grakn-test/test-integration/src/test/bash:./grakn-dist/target/grakn-dist-test PACKAGE=./grakn-dist/target/grakn-dist-test WORKSPACE=. ./grakn-test/test-snb/src/main/bash/validate.sh'
                    }
                }
                stage('Biomed') {
                    steps {
                        sh 'PATH=$PATH:./grakn-dist/target/grakn-dist-test ./grakn-test/test-biomed/load.sh'
                        sh 'PATH=$PATH:./grakn-dist/target/grakn-dist-test ./grakn-test/test-biomed/validate.sh'
                    }
                }

                stage('Long-running Instance (if stable branch)') {
                    when { branch 'stable' }

                    steps {
                        sshagent(credentials: ['jenkins-aws-ssh']) {
                            sh "echo 'Running the long running instance test in ${LONG_RUNNING_INSTANCE_ADDRESS}"
                            sh "scp -o StrictHostKeyChecking=no grakn-dist/target/grakn-dist-test.tar.gz ubuntu@${LONG_RUNNING_INSTANCE_ADDRESS}:~/grakn-dist.tar.gz"
                            sh "scp -o StrictHostKeyChecking=no scripts/repeat-query ubuntu@${LONG_RUNNING_INSTANCE_ADDRESS}:~/"
                            sh "ssh -o StrictHostKeyChecking=no -l ubuntu ${LONG_RUNNING_INSTANCE_ADDRESS} 'bash -s' < scripts/start-long-running-instance.sh"
                        }
                    }
                }
            }
        }

        stage('E2E Tests - grakn server stop') {
            steps {
                sh "cd grakn-dist/target/grakn-dist-test/ && ./grakn server stop"
                sh "cd grakn-dist/target/ && rm -r grakn-dist-test"
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