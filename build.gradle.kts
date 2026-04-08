plugins {
    java
    application
    checkstyle
    pmd
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    checkstyle("com.puppycrawl.tools:checkstyle:13.3.0")
    implementation("com.github.spotbugs:spotbugs-annotations:4.9.8")

    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("ch.qos.logback:logback-classic:1.5.32")
    implementation("com.h2database:h2:2.4.240")
    implementation("com.zaxxer:HikariCP:7.0.2")

    testImplementation(platform("org.junit:junit-bom:6.0.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
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
