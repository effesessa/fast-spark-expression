package com.effe.fast_spark_expression;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.effe.fast_spark_expression.core.SparkExpressionFilter;

/**
 * 
 * @author effe
 *
 */
public class App {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().appName("SparkExpression App").master("local[*]").getOrCreate();
		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/");

		// get singleton instance SparkExpressionFilter
		SparkExpressionFilter sparkExpressionFilter = SparkExpressionFilter.getInstance();

		//standard filter dataset without the use of static import functions :O
		dataset.filter(dataset.col("Type").equalTo("C").and(dataset.col("Calories").cast("int").$less(100))
				.and(dataset.col("Manufacturer").equalTo("Kelloggs").or(dataset.col("Manufacturer").equalTo("Nabisco")))
				.and(functions.not(dataset.col("Fat").cast("int").$greater(0)))).show(false);
		
		//standard filter dataset with the use of import static functions :O
		dataset.filter(col("Type").equalTo("C").and(col("Calories").cast("int").$less(100))
				.and(col("Manufacturer").equalTo("Kelloggs").or(col("Manufacturer").equalTo("Nabisco")))
				.and(not(col("Fat").cast("int").$greater(0)))).show(false);

		// filter dataset with SparkExpressionFilter
		sparkExpressionFilter.filter(dataset, "Type=C&(Calories<100)&(Manufacturer=Kelloggs|(Manufacturer=Nabisco))&(!(Fat>0))").show(false);
		// ENJOY! :)
		
		sparkSession.stop();
	}
}
