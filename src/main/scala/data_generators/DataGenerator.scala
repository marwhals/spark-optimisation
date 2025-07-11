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

  /**
   * For the RDD joins & cogroup lectures. Generates 3 files:
   * 1) with student IDs and names
   * 2) with student IDs and emails
   * 3) with student IDs and exam attempt grade
   *
   * @param rootFolderPath the path where the 3 files will be written
   * @param nStudents the number of students
   * @param nAttempts the number of attempts of the exam, per each student
   */
  def generateExamData(rootFolderPath: String, nStudents: Int, nAttempts: Int): Unit = {
    val studentNames = (0 to nStudents).map(_ => randomString(16))
    val studentIds = studentNames.map(_ => randomLong())
    val idWriter = new PrintWriter(new FileWriter(new File(s"$rootFolderPath/examIds.txt")))
    val emailWriter = new PrintWriter(new FileWriter(new File(s"$rootFolderPath/examEmails.txt")))
    val scoreWriter = new PrintWriter(new FileWriter(new File(s"$rootFolderPath/examScores.txt")))

    studentNames
      .zip(studentIds)
      .foreach {
        case (name, id) =>
          idWriter.println(s"$id $name")
          emailWriter.println(s"$id $name@rockthejvm.com")
      }

    val scores = studentIds
      .flatMap(id => Seq.fill(5)(id))
      .map(id => (id, randomInt(10), randomInt(10)))
      .toSet

    scores.foreach {
      case (id, scoreMaj, scoreMin) => scoreWriter.println(s"$id $scoreMaj.$scoreMin")
    }

    idWriter.flush()
    idWriter.close()
    emailWriter.flush()
    emailWriter.close()
    scoreWriter.flush()
    scoreWriter.close()
  }

  def main(args: Array[String]): Unit = {
  }
}