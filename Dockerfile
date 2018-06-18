FROM openjdk:8-jdk-alpine
COPY target/equitybot-data-svc-1.0.jar app.jar
EXPOSE 9011
ENTRYPOINT ["java","-Dspring.profiles.active=kubernetes","-Djava.security.egd=file:/dev/./urandom","-jar","app.jar"]