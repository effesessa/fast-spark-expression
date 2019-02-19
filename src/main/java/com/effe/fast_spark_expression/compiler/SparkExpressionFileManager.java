package com.effe.fast_spark_expression.compiler;

import java.io.IOException;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;

/**
 * 
 * @author effe
 *
 */
public class SparkExpressionFileManager extends ForwardingJavaFileManager<JavaFileManager> {
	
    private SparkExpressionClassLoader classLoader;

    public SparkExpressionFileManager(JavaFileManager fileManager, SparkExpressionClassLoader classLoader) {
        super(fileManager);
        this.classLoader = classLoader;
    }

    @Override
    public FileObject getFileForInput(Location location, String packageName, String relativeName) throws IOException {
        return classLoader.getStringJavaCode(relativeName);
    }

    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String qualifiedName, Kind kind, FileObject outputFile) throws IOException {
        return classLoader.getStringJavaCode(qualifiedName);
    }

    @Override
    public ClassLoader getClassLoader(Location location) {
        return classLoader;
    }
}