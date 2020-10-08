plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  testImplementation("junit:junit:4.11")
}

description = "traceable-raw-span-constants"
