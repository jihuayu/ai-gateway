plugins {
    `java-library`
    application
    alias(libs.plugins.graalvm.native)
}

application {
    mainClass.set("com.jihuayu.aigateway.netty.NettyGatewayMain")
    applicationDefaultJvmArgs = listOf("--enable-preview")
}

dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    api(project(":gateway-common"))

    // Direct Netty 4.2 HTTP & SSE Data-Plane Engine
    api(libs.netty.codec.http)
    api(libs.netty.handler)
    api(libs.netty.transport)
}

graalvmNative {
    metadataRepository {
        enabled.set(false)
    }
    binaries {
        named("main") {
            imageName.set("ai-gateway-netty")
            mainClass.set("com.jihuayu.aigateway.netty.NettyGatewayMain")
            sharedLibrary.set(false)
            verbose.set(true)
            buildArgs.addAll(
                "-Ob",
                "--enable-preview",
                "--enable-url-protocols=http,https",
                "-H:+ReportExceptionStackTraces",
                "--initialize-at-run-time=io.netty",
            )
        }
    }
}
