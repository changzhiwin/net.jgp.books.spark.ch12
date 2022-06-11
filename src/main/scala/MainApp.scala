package net.jgp.books.spark

import net.jgp.books.spark.ch12.lab200_record_transformation.RecordTransformation
import net.jgp.books.sparkInAction.ch12.lab300_join.HigherEdInstitutionPerCounty

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("JOIN", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "JOIN"  =>   HigherEdInstitutionPerCounty.run()
      case _       =>   RecordTransformation.run()
    }
  }
}