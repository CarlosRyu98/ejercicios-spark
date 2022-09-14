package org.joins

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Joins extends App with org.sparksession.spark {

  import spark.sqlContext.implicits._

  val (empDF, deptDF) = dataLoad()

  innerJoin()
  fullOuterJoin()
  leftOuterJoin()
  rightOuterJoin()
  leftSemiJoin()
  leftAntiJoin()
  selfJoin()

  // Función para cargar los datos
  private def dataLoad(): (DataFrame, DataFrame) = {
    val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )

    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")
    val empDF = emp.toDF(empColumns: _*)

    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )

    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)

    (empDF, deptDF)
  }

  // Inner join:
  // Solo muestra los valores que están en ambos dfs
  private def innerJoin(): Unit = {
    println("Inner Join")
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
      .show(false)
  }

  // Left Outer Join:
  // Muestra todos los valores del primer df y sus matches en el segundo
  private def leftOuterJoin(): Unit = {
    println("Left Outer Join")
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
      .show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter")
      .show(false)
  }

  // Full Outer Join:
  // Muestra todos los valores, aunque queden nulos
  private def fullOuterJoin(): Unit = {
    println("Full Outer Join")
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer")
      .show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
      .show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter")
      .show(false)
  }

  // Right Outer Join:
  // Muestra todos los valores del segundo df y sus matches en el primero
  private def rightOuterJoin(): Unit = {
    println("Right Outer Join")
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right")
      .show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter")
      .show(false)
  }

  // Left Semi Join:
  // Un Left Joi que solo muestra las columnas del primer df
  private def leftSemiJoin(): Unit = {
    println("Left Semi Join")
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi")
      .show(false)
  }

  // Left Anti Join:
  // Solo muestra las columnas del primer df que no tienen match
  private def leftAntiJoin(): Unit = {
    println("Left Anti Join")
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti")
      .show(false)
  }

  // Self Join:
  // Sirve para unir el df consigo mismo
  private def selfJoin(): Unit = {
    println("Semi Join")
    empDF.as("emp1")
      .join(empDF.as("emp2"),
        col("emp1.superior_emp_id") === col("emp2.emp_id"), "inner")
      .select(col("emp1.emp_id"), col("emp1.name"),
        col("emp2.emp_id").as("superior_emp_id"),
        col("emp2.name").as("superior_emp_name"))
      .show(false)
  }


}
