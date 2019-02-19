package com.effe.fast_spark_expression.compiler;

import org.apache.spark.sql.Column;

/**
 * 
 * @author effe
 *
 */
public interface ISparkExpression {
	
	Column getColumnCondition();
}
