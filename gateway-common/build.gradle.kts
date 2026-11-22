plugins {
    `java-library`
}

dependencies {
    api(platform(libs.spring.boot.dependencies))

    // Redis Hot-Path Client & Netty Buffer/Transport
    api(libs.lettuce.core)

    // Jackson JSON
    api(libs.jackson.databind)
    api(libs.jackson.datatype.jsr310)

    // Official Spring AI Core Model Metadata (Usage, ChatOptions, RateLimit, ChatResponseMetadata)
    api(libs.spring.ai.model) {
        exclude(group = "org.springframework", module = "spring-context")
        exclude(group = "org.springframework", module = "spring-web")
    }
    api(libs.spring.ai.commons) {
        exclude(group = "org.springframework", module = "spring-context")
        exclude(group = "org.springframework", module = "spring-web")
    }

    // Cryptography (AES-256-GCM & Scrypt)
    api(libs.bouncycastle.prov)
}
