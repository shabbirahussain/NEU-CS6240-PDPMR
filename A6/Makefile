# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME=out/artifacts/task.jar
RMDFILE=report
SPARK_HOME=/usr/local/spark
HADOOP_HOME=/usr/local/hadoop
DEPENDENCIES=${SPARK_HOME}/jars/*:out:.
all: clean build pdf

build:
	mkdir -p out/artifacts
	mkdir -p target/scala-2.11
	scalac -cp $(DEPENDENCIES) -d $(JAR_NAME) src/main/scala/org/neu/pdpmr/tasks/*.scala

run: build
	$(SPARK_HOME)/bin/spark-submit --class org.neu.pdpmr.tasks.Main $(JAR_NAME)

pdf:
	Rscript -e "HADOOP_HOME='${HADOOP_HOME}'; NUM_MEASUREMENTS='${NUM_MEASUREMENTS}'; require(knitr); require(markdown); knit('$(RMDFILE).rmd', '$(RMDFILE).md'); markdownToPDF('$(RMDFILE).md', '$(RMDFILE).pdf', options=c('use_xhtml', 'base64_images')); browseURL(paste('file://', file.path(getwd(),'$(RMDFILE).pdf'), sep=''))"

clean:
	rm -rf target/*
