package fr.exemple.formation.dataframe.dataset

case class Client(clientId: String, name: String, age: Int) {

  def isLegalAge: Boolean = age >= 18

}
