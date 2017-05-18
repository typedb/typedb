stage ('Build') {
    node {
        checkout scm
        sh 'mvn install -T 2.0C -DskipTests=True -DskipITs=True -Dmaven.javadoc.skip=true -U'
        stash includes: '**', name: 'grakncode'
    }
}

stage ('Unit tests') {
    parallel (
        'tinker': { node {
            unstash 'grakncode'
            sh 'mvn test -Ptinker -pl grakn-graql'
            junit '**/target/surefire-reports/*.xml'
        }},
        'titan': { node {
            unstash 'grakncode'
            sh 'mvn test -Ptitan -pl grakn-graql'
            junit '**/target/surefire-reports/*.xml'
        }}
    )
}
