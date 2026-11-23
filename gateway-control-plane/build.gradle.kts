plugins {
    alias(libs.plugins.spring.boot)
}

springBoot {
    mainClass.set("com.jihuayu.aigateway.AiGatewayApplication")
}

tasks.withType<org.springframework.boot.gradle.tasks.run.BootRun>().configureEach {
    jvmArgs("--enable-preview", "--enable-native-access=ALL-UNNAMED")
}

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":gateway-common"))
    implementation(project(":gateway-data-plane"))

    // Spring Boot 4.2 Web MVC (RestClient, @HttpExchange, Control Plane API & Static Console)
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.jdbc)

    // jOOQ Type-Safe SQL DSL + Flyway Schema Migrations + PostgreSQL
    implementation(libs.spring.boot.starter.jooq)
    implementation(libs.jooq)
    implementation(libs.flyway.core)
    implementation(libs.flyway.database.postgresql)
    runtimeOnly(libs.postgresql)

    // Spring Modulith (Architectural Module Boundaries & Verification)
    implementation(libs.spring.modulith.core)
    implementation(libs.spring.modulith.starter.core)

    // Official Spring AI Model Metadata
    implementation(libs.spring.ai.model)

    // Optional Apache Kafka Outbox Client
    implementation(libs.kafka.clients)

    // Testing: JUnit 5, Spring Boot Test, Spring Modulith Verification & Testcontainers (PostgreSQL)
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.boot.testcontainers)
    testImplementation(libs.spring.modulith.starter.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
}
