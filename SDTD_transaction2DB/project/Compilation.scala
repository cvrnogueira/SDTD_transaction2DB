import sbt.Keys.{libraryDependencies, _}
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.assembly
import sbtassembly.{MergeStrategy, PathList}

object Compilation {

  lazy val apiName = "transactions2DB"
  lazy val suffix = ".jar"

  lazy val buildSettings = Seq(
    name := apiName,
    organization := "io.sdtd",
    version := "1.0.0"
  )

  val settings = Seq(
    ThisBuild / scalaVersion := "2.11.12",
    libraryDependencies ++= Dependencies.all,
    mainClass in (Compile, run) := Some("io.sdtd.Application"),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    autoScalaLibrary := false,
    assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false),
  )
}
