pipeline {
  agent any
  stages {
    stage ('Build') {
      steps {
        sh 'mvn install -T 2.0C -DskipTests=True -DskipITs=True -Dmaven.javadoc.skip=true -U'
      }
    }
    stage ('Unit tests') {
      steps {
        sh 'mvn test -Ptinker -pl grakn-test'
      }
    }
  }
}
