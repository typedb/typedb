FROM graknlabs/jenkins-base

ENV WORKSPACE $WORKSPACE
COPY . /grakn-src/
WORKDIR /grakn-src/
RUN mvn install -T 2.0C -DskipTests=True -DskipITs=True -Dmaven.javadoc.skip=true -U
