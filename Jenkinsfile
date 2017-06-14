#!groovy
//properties([pipelineTriggers([cron('H H/6 * * *')])])
node('slave1-dev-jenkins') {
    def workspace = pwd()
    try {

        dir('grakn') {
            checkout scm
            stage('Build Grakn') {
                sh 'npm config set registry http://registry.npmjs.org/'
                sh 'mvn clean install -DskipTests -B -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT'
            }
            stage('Init Grakn') {
                sh 'mkdir grakn-package'
                sh 'tar -xf grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C grakn-package'
                sh 'grakn-package/bin/grakn.sh start'
                // todo: remove this hack when bug fixed
                sh 'grakn-package/bin/graql.sh -e "match \$x;"'
            }
        }
        dir('benchmarking') {
            git url: 'https://github.com/sheldonkhall/benchmarking', branch: 'jenkins'

            dir('single-machine-graph-scaling') {
                stage('Scale Test') {
                    sh 'mvn clean -U package'
                    sh 'java -jar target/single-machine-graph-scaling-0.14.0-SNAPSHOT-allinone.jar'
                }
            }

            dir('impls-SNB') {
                stage('Build LDBC Connector') {
                    sh 'mvn -U clean install assembly:single'
                }
            }

            withEnv(['VALIDATION_DATA=/home/jenkins/readwrite_neo4j--validation_set.tar.gz',
                     'CSV_DATA=social_network',
                     'KEYSPACE=snb',
                     'ENGINE=localhost:4567',
                     'ACTIVE_TASKS=1000',
                     'PATH+EXTRA=' + workspace + '/grakn/grakn-package/bin',
                     'LDBC_DRIVER=/home/jenkins/ldbc_driver/target/jeeves-0.3-SNAPSHOT.jar',
                     'LDBC_CONNECTOR=' + workspace + '/benchmarking/impls-SNB/target/snb-interactive-grakn-0.0.1-jar-with-dependencies.jar',
                     'LDBC_VALIDATION_CONFIG=readwrite_grakn--ldbc_driver_config--db_validation.properties']) {
                dir('generate-SNB') {
                    stage('Load Validation Data') {
                        sh './load-SNB.sh arch validate'
                    }
                }
                stage('Measure Size') {
                    sh '../grakn/grakn-package/bin/nodetool flush'
                    sh 'du -hd 0 ../grakn/grakn-package/db/cassandra/data'
                }
                dir('validate-SNB') {
                    stage('Validate Graph') {
                        sh './validate.sh'
                    }
                }
            }
        }

    } finally {

        dir('grakn') {
            stage('Tear Down Grakn') {
                sh 'grakn-package/bin/grakn.sh stop'
                sh 'rm -rf grakn-package'
            }
        }

    }
}
