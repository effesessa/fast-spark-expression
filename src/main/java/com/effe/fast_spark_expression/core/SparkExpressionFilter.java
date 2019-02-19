package com.effe.fast_spark_expression.core;

import java.util.Arrays;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.effe.fast_spark_expression.compiler.ISparkExpression;
import com.effe.fast_spark_expression.compiler.SparkExpressionClassLoader;
import com.effe.fast_spark_expression.compiler.SparkExpressionCodeGenerator;
import com.effe.fast_spark_expression.compiler.SparkExpressionFileManager;
import com.effe.fast_spark_expression.compiler.SparkExpressionStringCode;
import com.effe.fast_spark_expression.parser.ColumnConditionParser;

import javassist.compiler.CompileError;

/**
 * 
 * @author effe
 *
 */
public class SparkExpressionFilter {
	
	private static final String BASE_CLASS_NAME_SPARK_EXPRESSION = "SparkExpression";
	
	private static SparkExpressionFilter instance;
	
	private JavaCompiler javaCompiler;
	
	private DiagnosticCollector<JavaFileObject> diagnostic;
	
	private SparkExpressionClassLoader sparkExpressionClassLoader;
	
	private SparkExpressionFileManager sparkExpressionFileManager;
	
	private int numberCondition;
	
	private SparkExpressionFilter() {
		javaCompiler = ToolProvider.getSystemJavaCompiler();
		diagnostic = new DiagnosticCollector<>();
		JavaFileManager javaFileManager = javaCompiler.getStandardFileManager(diagnostic, null, null);
		sparkExpressionClassLoader = new SparkExpressionClassLoader(this.getClass().getClassLoader());
		sparkExpressionFileManager = new SparkExpressionFileManager(javaFileManager, sparkExpressionClassLoader);
		numberCondition = 0;
	}
	
	public static SparkExpressionFilter getInstance() {
		return instance == null ? (instance = new SparkExpressionFilter()) : instance;
	}
	
	public Dataset<Row> filter(Dataset<Row> dataset, String condition) {
		String className = BASE_CLASS_NAME_SPARK_EXPRESSION + numberCondition;
		String parseredCondition = ColumnConditionParser.parse(condition);
		String code = SparkExpressionCodeGenerator.get(className, parseredCondition);
		SparkExpressionStringCode sparkExpressionStringCode = new SparkExpressionStringCode(className, code);
		sparkExpressionClassLoader.addClass(sparkExpressionStringCode);
		boolean call = javaCompiler.getTask(null, sparkExpressionFileManager, diagnostic, null, null, Arrays.asList(sparkExpressionStringCode)).call();
		ISparkExpression sparkExpression = loadClass(className, call);
		Column columnCondition = sparkExpression.getColumnCondition();
		numberCondition++;
		return dataset.filter(columnCondition);
	}

	private ISparkExpression loadClass(String className, boolean call) {
		try {
			if(call)
				return (ISparkExpression) sparkExpressionClassLoader.loadClass(className).newInstance();
			else
				throw new CompileError("compile error");
		} 
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException | CompileError  e) {
			System.err.println(e.getMessage());
		}
		return null;
	}
}
