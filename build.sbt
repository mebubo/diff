scalaVersion := "2.12.8"

scalacOptions += "-Ypartial-unification"

libraryDependencies += "org.typelevel" %% "cats-core" % "1.6.1"
libraryDependencies += "org.typelevel" %% "cats-free" % "1.6.1"
libraryDependencies += "org.typelevel" %% "cats-effect" % "1.3.1"

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")