// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "org.mellowtech"

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>https://github.com/msvens/sdb</url>
    <scm>
      <developerConnection>scm:git:git@github.com:msvens/sdb.git</developerConnection>
      <connection>scm:git:git@github.com:msvens/sdb.git</connection>
      <url>git@github.com:msvens/sdb.git</url>
    </scm>
    <licenses>
      <license>
        <name>The Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <developers>
      <developer>
        <name>Martin Svensson</name>
        <email>msvens@gmail.com</email>
        <organization>Mellowtech</organization>
        <organizationUrl>http://www.mellowtech.org/</organizationUrl>
      </developer>
    </developers>
}