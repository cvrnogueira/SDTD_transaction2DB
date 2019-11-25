package io.sdtd.dictonary

object Words {

   /*TODO:
       Improve this words list. One idea is to talk
       with people asking what are the words that you relate with "flu", for example.
       We have to use google translate API to translate it too
    */
  val diseases = List("Influenza", "flu", "chills", "headache", "sore throat",
    "runny nose", "sneezing", "fever", "dry cough", "cough").map(word => word.toLowerCase())

  val depression = List("suicide", "depressed", "depression", "kill myself").map(word => word.toLowerCase())

}
