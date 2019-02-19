# fast-spark-expression
Fast Spark Expression - Write column expressions quickly and easily like a string

## Motivation
Often, writing very large column expressions turns out to be a tedious and slow task.

## How does it work?
Read dataset
```javascript
Dataset<Row> dataset = ...
```
Get SparkExpressionFilter instance
```javascript
SparkExpressionFilter sparkExpressionFilter = SparkExpressionFilter.getInstance();
```
Filter fast-spark-expression way
```javascript
String expression = "Type=C&(Calories<100)&(Manufacturer=Kelloggs|(Manufacturer=Nabisco))&(!(Fat>0))";
sparkExpressionFilter.filter(dataset, expression).show(false);
```

## Tedious standard way
Filter standard way
```javascript
dataset.filter(dataset.col("Type").equalTo("C").and(dataset.col("Calories").cast("int").$less(100))
  .and(dataset.col("Manufacturer").equalTo("Kelloggs").or(dataset.col("Manufacturer").equalTo("Nabisco")))
  .and(functions.not(dataset.col("Fat").cast("int").$greater(0)))).show(false);
```
Filter standard way with import static functions
```javascript
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

dataset.filter(col("Type").equalTo("C").and(col("Calories").cast("int").$less(100))
  .and(col("Manufacturer").equalTo("Kelloggs").or(col("Manufacturer").equalTo("Nabisco")))
  .and(not(col("Fat").cast("int").$greater(0)))).show(false);
```
## Support
Actually, support:
isNotNull(), isNull(), equalTo(), notEqual(), $less(), $greater(), or(), and() and not()
####
Enjoy! :)
