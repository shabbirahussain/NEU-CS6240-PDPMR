RMDFILE=report

all: build run html

build:
	javac -d target/classes src/main/java/com/pdpmr/taskA1/util/*.java src/main/java/com/pdpmr/taskA1/collectors/*.java src/main/java/com/pdpmr/taskA1/mappers/*.java src/main/java/com/pdpmr/taskA1/subtasks/*.java src/main/java/com/pdpmr/taskA1/*.java

run: build exec

exec:
	java -Xmx8g -cp "./target/classes" com.pdpmr.taskA1.Main

clean:
	rm -rf target/classes
	rm -rf input/*.utf-8

html :
	Rscript -e "require(knitr); require(markdown); knit('$(RMDFILE).rmd', '$(RMDFILE).md'); markdownToHTML('$(RMDFILE).md', '$(RMDFILE).html', options=c('use_xhtml', 'base64_images')); browseURL(paste('file://', file.path(getwd(),'$(RMDFILE).html'), sep=''))"