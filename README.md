# 远程调试
## attach模式
```
bin/spark-submit --driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" --class com.slutsky.scala_demo.WordCount --master local examples/jars/scala_demo-0.0.1-SNAPSHOT.jar
```
# POM.xml
## 注释
```
<arg>-make:transitive</arg>
```