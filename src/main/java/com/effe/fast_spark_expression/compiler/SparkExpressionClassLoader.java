package com.effe.fast_spark_expression.compiler;

import java.util.Map;

import org.spark_project.guava.collect.Maps;

/**
 * 
 * @author effe
 *
 */
public class SparkExpressionClassLoader extends ClassLoader {

	private Map<String, SparkExpressionStringCode> classes;
	
	public SparkExpressionClassLoader(ClassLoader parentClassLoader) {
		super(parentClassLoader);
		this.classes = Maps.newHashMap();
	}
	
	public void addClass(SparkExpressionStringCode stringJavaCode) {
		classes.put(stringJavaCode.getName(), stringJavaCode);
	}
	
	public SparkExpressionStringCode getStringJavaCode(String name) {
		return classes.get(name);
	}
	
	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		 byte[] bytes = classes.get(name).getBytes();
		 return defineClass(name, bytes, 0, bytes.length);
	}
}
