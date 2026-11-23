plugins {
    `java-library`
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(
        listOf(
            "--add-modules",
            "org.graalvm.nativeimage,org.graalvm.word"
        )
    )
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

    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.register<JavaExec>("compilePluginSharedLib") {
    group = "native"
    description = "Compiles an unmodified GatewayPlugin .jar into a GraalVM --shared native library (.so/.dylib/.dll)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.jihuayu.aigateway.plugin.JarToSharedLibCompiler")
    jvmArgs("--enable-preview", "--enable-native-access=ALL-UNNAMED")
    environment("JAVA_HOME", System.getProperty("java.home"))
    val jarProp = providers.gradleProperty("pluginJar").orElse(providers.gradleProperty("jar")).orNull
    val outProp = providers.gradleProperty("pluginOut").orElse(providers.gradleProperty("out")).orNull
    if (jarProp != null) {
        args(listOfNotNull(jarProp, outProp))
    }
}
