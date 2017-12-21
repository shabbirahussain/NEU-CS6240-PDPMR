# Environment configurations
#HADOOP_HOME=/Users/shabbirhussain/Apps/hadoop-2.8.1

IN_DIR_ON_HDFS=/input/
OUT_DIR_ON_HDFS=/output/
LOCAL_OUT_DIR=out/results/

# Experiment configurations
QUERY_FILE="input/query.csv"


# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME="out/artifacts/task.jar"
RMDFILE="report"
NUM_MAPPER=10
NUM_REDUCER=10
HADOOP_CLIENT_OPTS="-Xmx12g"

all: clean setup build run teardown

build:
	mkdir -p "out/artifacts"
	mkdir -p "target/classes"
	javac -cp "./lib/*:${HADOOP_HOME}/share/hadoop/common/lib/*" \
		-d target/classes \
		src/main/java/org/neu/pdpmr/tasks/**/*.java \
		src/main/java/org/neu/pdpmr/tasks/*.java
	jar cvfm ${JAR_NAME} \
		src/main/java/META-INF/MANIFEST.MF \
		-C target/classes/ .
	jar uvf ${JAR_NAME} resources/packaged/*
	jar uvf ${JAR_NAME} lib/*

run: html

exec: build
	echo "${HADOOP_CLIENT_OPTS}"
	${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} org.neu.pdpmr.tasks.Main 	\
		-libjars $(shell ls lib/*.jar  | tr '\n' ',')                  	\
		-D mapreduce.job.maps=${NUM_MAPPER}                           	\
		-D mapreduce.job.reduces=${NUM_REDUCER} 					   	\
		-D mapred.child.java.opts=7g									\
		-queryFile=${QUERY_FILE}										\
		-inDir=${IN_DIR_ON_HDFS}										\
		-outDir=${OUT_DIR_ON_HDFS}										\
		-localOutDir=${LOCAL_OUT_DIR}

html:
	Rscript -e "HADOOP_HOME='${HADOOP_HOME}'; NUM_MEASUREMENTS='${NUM_MEASUREMENTS}'; require(knitr); require(markdown); knit('$(RMDFILE).rmd', '$(RMDFILE).md'); markdownToHTML('$(RMDFILE).md', '$(RMDFILE).html', options=c('use_xhtml', 'base64_images')); browseURL(paste('file://', file.path(getwd(),'$(RMDFILE).html'), sep=''))"

clean:
	rm -rf target/classes/*
	${HADOOP_HOME}/bin/hadoop fs -rm -r /output || true

setup:
	${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${IN_DIR_ON_HDFS}
	${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${OUT_DIR_ON_HDFS}
	${HADOOP_HOME}/bin/hadoop fs -copyFromLocal -f input/* ${IN_DIR_ON_HDFS}

teardown:
	${HADOOP_HOME}/bin/hadoop fs -rm -r ${IN_DIR_ON_HDFS}
