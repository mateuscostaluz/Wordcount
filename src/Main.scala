package br.ufrj.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Main extends App {

  // Define o nível de log para imprimir apenas erros
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Cria um SparkContext usando a máquina local
  val sc = new SparkContext("local", "Wordcount")

  val books = Array("Beyond Good and Evil - Friedrich Nietzsche.txt",
                    "Frankenstein The Modern Prometheus by Mary Wollstonecraft Shelley.txt",
                    "Grimms Fairy Tales by Jacob Grimm and Wilhelm Grimm.txt",
                    "Metamorphosis by Franz Kafka.txt",
                    "The Prince by Niccolò Machiavelli.txt",
                    "The Works of Edgar Allan Poe Volume 2 by Edgar Allan Poe.txt")

  def readLines(book: String): RDD[(String)] = {
    // Carrega cada linha do livro em um RDD
    val input = sc.textFile(s"data/${book}")

    // Divide usando uma expressão regular que extrai palavras e aplica um filtro para não considerar linhas vazias
    val words = input.flatMap(x => x.split("\\W+")).filter(x => x.nonEmpty)

    // Normaliza tudo em letras minúsculas
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Coleta em Array para não retornar um objeto MapPartitionsRDD
    val lowercaseWordsArray = lowercaseWords.collect()

    // Converte novamente para retornar um objeto RDD
    sc.parallelize(lowercaseWordsArray)
  }

  // Cria um RDD vazio para receber as palavras de todos os livros
  var lowercaseWords = sc.emptyRDD[(String)]

  // Percorre a lista imutável de livros e aplica o método de separação das palavras
  for (book <- books) {
    val lines = readLines(book)
    lowercaseWords = lowercaseWords.union(lines)
  }

  // Conta as ocorrências de cada palavra
  val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)

  // Inverte (word, count) para (count, word) e, em seguida, classifica por chave (count)
  val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

  // Imprime os resultados invertendo novamente (count, word) para (word, count) à medida que avança no loop
  for (result <- wordCountsSorted) {
    val count = result._1
    val word = result._2
    println(s"$word: $count")
  }

}
