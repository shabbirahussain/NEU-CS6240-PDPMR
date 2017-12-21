INPUT_PATH="input/"

SPARK_BIN_PATH=/Users/shabbirhussain/Apps/spark-2.2.0-bin-hadoop2.7/bin/
SCALA_BIN_PATH=/usr/local/Cellar/scala@2.11/2.11.11/bin/
#
SPARK_HOME=/usr/local/spark
HADOOP_HOME=/usr/local/hadoop
CLASSPATH=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${HADOOP_HOME}/share/hadoop/mapreduce/*:${SPARK_HOME}/jars/*:out:.
BASE_DIR=src/main/scala/org/neu
INPUT=input/
JAR_PATH=out/Main1.jar
MAIN_CLASS=org.neu.Main1
#


# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME="out/artifacts/task.jar"
LIB_PATH=lib-min

all: clean run report

build:
	mkdir -p "out/artifacts"
	mkdir -p "out/classes/main/resources/"
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d out/classes \
		src/main/scala/org/neu/pdpmr/tasks/**/*.scala \
		src/main/scala/org/neu/pdpmr/tasks/*.scala
	cp src/main/resources/* out/classes/
	jar cvfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C out/classes/ . \
		&>/dev/null

run: build
	${SPARK_PATH}spark-submit \
	 	--master local --driver-memory 6g \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    	--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    	--class org.neu.pdpmr.tasks.Main ${JAR_NAME} ${INPUT_PATH}

report:
	Rscript -e "rmarkdown::render('Report.Rmd')"

clean:
	-rm -rf out/*

sub1: build1 run1

build1:
	mkdir -p out
	scalac -cp ${CLASSPATH} -d ${JAR_PATH} ${BASE_DIR}/*.scala

run1:
	${SPARK_HOME}/bin/spark-submit --class ${MAIN_CLASS} ${JAR_PATH} ${INPUT}


report1:
	Rscript -e "rmarkdown::render('report.rmd')"
