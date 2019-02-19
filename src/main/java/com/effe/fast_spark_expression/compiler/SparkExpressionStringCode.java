package com.effe.fast_spark_expression.compiler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

/**
 * 
 * @author effe
 *
 */
public class SparkExpressionStringCode extends SimpleJavaFileObject {

	private String code, name;
	
	private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	
	public SparkExpressionStringCode(String name, String code) {
		super(URI.create("string:///" + name.replace(".", "/") + Kind.SOURCE.extension), Kind.SOURCE);
		this.code = code;
		this.name = name;
	}
	
	@Override
	public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
		return code;
	}
	
	@Override
	public OutputStream openOutputStream() throws IOException {
		return outputStream;
	}
	
	public byte[] getBytes() {
		return outputStream.toByteArray();
	}
	
	public String getName() {
		return name;
	}
}
