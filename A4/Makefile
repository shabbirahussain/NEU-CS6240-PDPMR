# Environment configurations
HADOOP_HOME=/Users/shabbirhussain/Apps/hadoop-2.8.1

DATA_DIR_ON_HDFS=/input/
#books/10.txt.utf-8
OUT_DIR_ON_HDFS=/output

# Experiment configurations
NUM_WORKERS=4
TOP_N=5
DATA_FROM_YEAR=1989

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME="out/artifacts/task.jar"
RMDFILE="report"

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
	${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} org.neu.pdpmr.tasks.Main \
		-libjars $(shell ls lib/*.jar  | tr '\n' ',')                  \
		-D mapreduce.job.maps=${NUM_WORKERS}                           \
		-D mapreduce.job.reduces=${NUM_WORKERS} 					   \
		-D mapreduce.reduce.memory.mb=8000 							   \
		-D mapreduce.reduce.java.opts=-Xmx6608m                        \
		-datDir=${DATA_DIR_ON_HDFS}                                    \
		-n=${TOP_N}													   \
		-outDir=${OUT_DIR_ON_HDFS}									   \
		-yearsSince=${DATA_FROM_YEAR}
	${HADOOP_HOME}/bin/hadoop fs -getmerge ${OUT_DIR_ON_HDFS}/ out/results/task_out.csv

html:
	Rscript -e "HADOOP_HOME='${HADOOP_HOME}'; NUM_MEASUREMENTS='${NUM_MEASUREMENTS}'; require(knitr); require(markdown); knit('$(RMDFILE).rmd', '$(RMDFILE).md'); markdownToHTML('$(RMDFILE).md', '$(RMDFILE).html', options=c('use_xhtml', 'base64_images')); browseURL(paste('file://', file.path(getwd(),'$(RMDFILE).html'), sep=''))"

clean:
	rm -rf target/classes/*
	${HADOOP_HOME}/bin/hadoop fs -rm -r /output || true

setup:
	${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${DATA_DIR_ON_HDFS}
	${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${OUT_DIR_ON_HDFS}
	${HADOOP_HOME}/bin/hadoop fs -copyFromLocal -f input/* ${DATA_DIR_ON_HDFS}

teardown:
	${HADOOP_HOME}/bin/hadoop fs -rm -r ${DATA_DIR_ON_HDFS}
