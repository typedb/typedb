stage ('Build') {
  node {
    sh 'npm config set registry http://registry.npmjs.org/'
    checkout scm
    sh "mvn versions:set -DnewVersion=${env.BRANCH_NAME} -DgenerateBackupPoms=false"
    sh 'mvn clean package -DskipTests -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT'
    archiveArtifacts artifacts: 'grakn-dist/target/grakn-dist*.tar.gz'
  }
}
