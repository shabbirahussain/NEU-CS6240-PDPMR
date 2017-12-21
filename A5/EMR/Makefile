IN_DIR_ON_HDFS=s3://pdpmrf17/
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

exec:
	export HADOOP_CLIENT_OPTS="-Xmx16g $HADOOP_CLIENT_OPTS"
	hadoop jar ${JAR_NAME} org.neu.pdpmr.tasks.Main 		\
		-libjars $(shell ls lib/*.jar  | tr '\n' ',')       \
		-D mapreduce.job.maps=${NUM_MAPPER}                 \
		-D mapreduce.job.reduces=${NUM_REDUCER} 			\
		-D mapreduce.reduce.memory.mb=5120 					\
		-D mapreduce.reduce.java.opts=-Xmx4608m 			\
		-D mapreduce.mapper.memory.mb=5120 					\
		-D mapreduce.mapper.java.opts=-Xmx4608m 			\
		-queryFile=${QUERY_FILE}							\
		-inDir=${IN_DIR_ON_HDFS}							\
		-outDir=${OUT_DIR_ON_HDFS}							\
		-localOutDir=${LOCAL_OUT_DIR}
setup:
	hadoop fs -mkdir -p ${OUT_DIR_ON_HDFS}
