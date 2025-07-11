package data_generators

import java.io.{File, FileWriter, PrintWriter}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

object DataGenerator {

  val random = new Random()

  /////////////////////////////////////////////////////////////////////////////////
  // General data generation
  /////////////////////////////////////////////////////////////////////////////////

  def randomDouble(limit: Double): Double = random.nextDouble() * limit

  def randomLong(limit: Long = Long.MaxValue): Long = Math.abs(random.nextLong()) % limit

  def randomInt(limit: Int = Int.MaxValue): Int = random.nextInt(limit)

  def randomIntBetween(low: Int, high: Int) = {
    assert(low <= high)
    random.nextInt(high - low) + low
  }

  def randomString(n: Int) =
    new String((0 to n).map(_ => ('a' + random.nextInt(26)).toChar).toArray)

  /////////////////////////////////////////////////////////////////////////////////
  // Laptop models generation - skewed data lectures
  /////////////////////////////////////////////////////////////////////////////////

  val laptopModelsSet: Seq[LaptopModel] = Seq(
    LaptopModel("Razer", "Blade"),
    LaptopModel("Alienware", "Area-51"),
    LaptopModel("HP", "Omen"),
    LaptopModel("Acer", "Predator"),
    LaptopModel("Asus", "ROG"),
    LaptopModel("Lenovo", "Legion"),
    LaptopModel("MSI", "Raider")
  )

  def randomLaptopModel(uniform: Boolean = false): LaptopModel = {
    val makeModelIndex = if (!uniform && random.nextBoolean()) 0 else random.nextInt(laptopModelsSet.size) // 50% of the data is of the first kind
    laptopModelsSet(makeModelIndex)
  }

  def randomProcSpeed() = s"3.${random.nextInt(9)}".toDouble

  def randomRegistration(): String = s"${random.alphanumeric.take(7).mkString("")}"

  def randomPrice() = 500 + random.nextInt(1500)

  def randomLaptop(uniformDist: Boolean = false): Laptop = {
    val makeModel = randomLaptopModel()
    Laptop(randomRegistration(), makeModel.make, makeModel.model, randomProcSpeed())
  }

  def randomLaptopOffer(uniformDist: Boolean = false): LaptopOffer = {
    val makeModel = randomLaptopModel()
    LaptopOffer(makeModel.make, makeModel.model, randomProcSpeed(), randomPrice())
  }

  def main(args: Array[String]): Unit = {
  }
}