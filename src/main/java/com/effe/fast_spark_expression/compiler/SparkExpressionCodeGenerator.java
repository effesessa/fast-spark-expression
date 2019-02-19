package com.effe.fast_spark_expression.compiler;

/**
 * 
 * @author effe
 *
 */
public class SparkExpressionCodeGenerator {
	
	public static String get(String className, String condition) {
		return (new StringBuilder()
			.append("import com.effe.fast_spark_expression.compiler.ISparkExpression;")
			.append("import org.apache.spark.sql.Column;")
			.append("import static org.apache.spark.sql.functions.not;")
			.append("import static org.apache.spark.sql.functions.col;")
			.append("public class " + className + " implements ISparkExpression {")
			.append("	public Column getColumnCondition() {")
			.append("		return " + condition + ";")
			.append("	}")
			.append("}")).toString();
	}
}
