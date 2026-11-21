FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src
ARG START_CLASS=org.aldorion.module1practice.Module1PracticeApplication
RUN chmod +x mvnw && ./mvnw -B -Dstart-class="$START_CLASS" -Dmaven.test.skip=true package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/Module-1-Practice-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
