# Environment configurations
HADOOP_HOME=/Users/shabbirhussain/Apps/hadoop-2.8.1

DATA_DIR_ON_HDFS=/input
OUT_DIR_ON_HDFS=/user/shabbirhussain/tasks/a2/output

# Experiment configurations
NUM_WORKERS=1
NUM_MEASUREMENTS = 1
K_NEIGHBORHOOD_SIZE=3

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME="out/artifacts/shabbir_task_a2.jar"
RMDFILE="report"

all: clean setup build run teardown

build:
	javac -cp "./lib/*:${HADOOP_HOME}/share/hadoop/common/lib/*" \
		-d target/classes \
		src/main/java/org/neu/pdpmr/tasks/**/*.java \
		src/main/java/org/neu/pdpmr/tasks/*.java
	jar cvfm ${JAR_NAME} \
		src/main/java/META-INF/MANIFEST.MF \
		-C target/classes/ .

run: html

exec: build
	${HADOOP_HOME}/bin/hadoop jar ${JAR_NAME} org.neu.pdpmr.tasks.Main \
		-libjars $(shell ls lib/*.jar  | tr '\n' ',')                  \
		-D mapreduce.job.maps=${NUM_WORKERS}                           \
		-datDir=${DATA_DIR_ON_HDFS}                                    \
		-outDir=${OUT_DIR_ON_HDFS}                                     \
		-k=${K_NEIGHBORHOOD_SIZE}
	${HADOOP_HOME}/bin/hadoop fs -get ${OUT_DIR_ON_HDFS}/part-r-00000 out/results/result_taskA2.csv

html:
	Rscript -e "HADOOP_HOME='${HADOOP_HOME}'; NUM_MEASUREMENTS='${NUM_MEASUREMENTS}'; require(knitr); require(markdown); knit('$(RMDFILE).rmd', '$(RMDFILE).md'); markdownToHTML('$(RMDFILE).md', '$(RMDFILE).html', options=c('use_xhtml', 'base64_images')); browseURL(paste('file://', file.path(getwd(),'$(RMDFILE).html'), sep=''))"

clean:
	rm -rf target/classes/*
	${HADOOP_HOME}/bin/hadoop fs -rm -r -f /user/shabbirhussain/taskA2 || true
	${HADOOP_HOME}/bin/hadoop fs -rm -r output || true
	${HADOOP_HOME}/bin/hadoop fs -rm -r letter-count || true

setup:
	${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${DATA_DIR_ON_HDFS}
	${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${OUT_DIR_ON_HDFS}
	${HADOOP_HOME}/bin/hadoop fs -copyFromLocal -f input/* ${DATA_DIR_ON_HDFS}

teardown:
	${HADOOP_HOME}/bin/hadoop fs -rm -r ${DATA_DIR_ON_HDFS}
