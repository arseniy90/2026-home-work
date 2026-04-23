import com.google.protobuf.gradle.id

plugins {
    java
    application
    checkstyle
    pmd

    id("com.google.protobuf") version "0.10.0"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}


val reactiveGrpcVersion = "1.2.4"
val grpcVersion = "1.58.0"
val protobufVersion = "3.4.0"

dependencies {
    checkstyle("com.puppycrawl.tools:checkstyle:13.3.0")
    implementation("com.github.spotbugs:spotbugs-annotations:4.9.8")

    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("ch.qos.logback:logback-classic:1.5.32")
    implementation("com.h2database:h2:2.4.240")
    implementation("com.zaxxer:HikariCP:7.0.2")

    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")
    implementation("com.google.protobuf:protobuf-java-util:${protobufVersion}")
    implementation("com.salesforce.servicelibs:reactor-grpc-stub:${reactiveGrpcVersion}")
    implementation("io.grpc:grpc-netty-shaded:${grpcVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-services:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
    implementation("io.projectreactor:reactor-core:3.8.5")

    testImplementation(platform("org.junit:junit-bom:6.0.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${protobufVersion}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}"
        }
        id("reactor-grpc") {
            artifact = "com.salesforce.servicelibs:reactor-grpc:${reactiveGrpcVersion}"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
                create("reactor-grpc")
            }
        }
    }
}

sourceSets {
    create("integrationTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}
val integrationTestImplementation by configurations.getting {
    extendsFrom(configurations.implementation.get(), configurations.testImplementation.get())
}
val integrationTestRuntimeOnly by configurations.getting

configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get(), configurations.testRuntimeOnly.get())


tasks.test {
    maxHeapSize = "128m"
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests."
    group = "verification"

    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    shouldRunAfter("test")

    useJUnitPlatform()
    maxHeapSize = "128m"

    testLogging {
        events("passed")
    }
}

application {
    mainClass = "company.vk.edu.distrib.compute.Server"
    applicationDefaultJvmArgs = listOf("-Xmx128m")
}

checkstyle {
    configFile = project.layout.projectDirectory.file("checkstyle.xml").asFile
    maxWarnings = 0
}


pmd {
    isConsoleOutput = true
    toolVersion = "7.16.0"
    rulesMinimumPriority = 5
    ruleSetFiles(project.layout.projectDirectory.file("pmd.xml"))
}

tasks.register("codeStyleChecks") {
    group = "verification"
    dependsOn(
        "checkstyleMain",
        "checkstyleTest",
        "checkstyleIntegrationTest",
        "pmdMain",
    )
}

tasks.check {
    dependsOn(tasks.test, integrationTest, "codeStyleChecks")
}

tasks.named("pmdIntegrationTest") {
    enabled = false
}

tasks.named("pmdTest") {
    enabled = false
}
