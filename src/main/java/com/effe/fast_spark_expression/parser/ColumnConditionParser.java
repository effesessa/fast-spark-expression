package com.effe.fast_spark_expression.parser;

/**
 * 
 * @author effe
 *
 */
public class ColumnConditionParser {
	
	public static String parse(String condition) {
		return condition.trim()
			.replaceAll("([a-zA-Z0-9_-]+)=null", "col(\"$1\").isNull()")
			.replaceAll("([a-zA-Z0-9_-]+)!=null", "col(\"$1\").isNotNull()")
			.replaceAll("([a-zA-Z0-9_-]+)=([a-zA-Z0-9#@\\.\\s\\_\\-]+)", "col(\"$1\").equalTo(\"$2\")")
			.replaceAll("([a-zA-Z0-9_-]+)!=([a-zA-Z0-9@#\\.\\s\\_\\-]+)", "col(\"$1\").notEqual(\"$2\")")
			.replaceAll("([a-zA-Z0-9_-]+)<(\\d+)", "col(\"$1\").cast(\"int\").\\$less($2)")
			.replaceAll("([a-zA-Z0-9_-]+)>(\\d+)", "col(\"$1\").cast(\"int\").\\$greater($2)")
			.replaceAll("!", "not")
			.replaceAll("&", ".and")
			.replaceAll("\\|", ".or");
	}
}
