import com.diffplug.gradle.spotless.SpotlessExtension

plugins {
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.graalvm.native) apply false
}

allprojects {
    group = "com.jihuayu"
    version = "0.2.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(25)
        options.compilerArgs.add("--enable-preview")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs("--enable-preview")
    }

    extensions.configure<SpotlessExtension> {
        java {
            target("src/**/*.java")
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }
}
