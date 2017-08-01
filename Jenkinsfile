node {
  //Everything is wrapped in a try catch so we can handle any test failures
  //If one test fails then all the others will stop. I.e. we fail fast
  try {
    def workspace = pwd()
    //Always wrap each test block in a timeout
    //This first block sets up engine within 15 minutes
    timeout(15) {
      stage('Build Grakn') {//Stages allow you to organise and group things within Jenkins
        sh 'npm config set registry http://registry.npmjs.org/'
        checkout scm
        sh 'if [ -d maven ] ;  then rm -rf maven ; fi'
        sh "mvn versions:set -DnewVersion=${env.BRANCH_NAME} -DgenerateBackupPoms=false"
        sh 'mvn clean package -DskipTests -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT'
        archiveArtifacts artifacts: "grakn-dist/target/grakn-dist*.tar.gz"
      }
      stage('Init Grakn') {
        sh 'if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi'
        sh 'mkdir grakn-package'
        sh 'tar -xf grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C grakn-package'
        sh 'grakn-package/bin/grakn.sh start'
      }
      stage('Test Connection') {
        sh 'grakn-package/bin/graql.sh -e "match \\\$x;"' //Sanity check query. I.e. is everything working?
      }
      stage('Build LDBC Driver') {
        dir('ldbc-driver') {
          git url: 'https://github.com/ldbc/ldbc_driver', branch: 'master'
          sh 'mvn -U clean install -DskipTests -Dmaven.repo.local=' + workspace + '/maven '
        }
      }
    }
    slackSend channel: "#github", message: "Periodic Build Success on " + buildBranch + ": ${env.BUILD_NUMBER} (<${env.BUILD_URL}flowGraphTable/|Open>)"
  } catch (error) {
    slackSend channel: "#github", message: "Periodic Build Failed on " + buildBranch + ": ${env.BUILD_NUMBER} (<${env.BUILD_URL}flowGraphTable/|Open>)"
    throw error
  } finally { // Tears down test environment
    timeout(5) {
      stage('Tear Down Grakn') {
        sh 'if [ -d maven ] ;  then rm -rf maven ; fi'
        archiveArtifacts artifacts: 'grakn-package/logs/grakn.log'
        sh 'grakn-package/bin/grakn.sh stop'
        sh 'if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi'
      }
    }
  }
}