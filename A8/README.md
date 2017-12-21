# Objective

Do artist clustering. More details can be found [here](http://janvitek.org/pdpmr/f17/task-a7-clustering.html).

## Setup

* Scala and Spark installation required. Please update paths if not on path variable in the Makefile.
* Please install the required R-libraries before executing the code. List of libs can be found at top of the `Report.rmd` in the `r setup` block with library statements.
* This program expects inputs to be available in `input/` and `input/all` directory as set of 3 files:
	- `artist_terms.csv.gz`
	- `similar_artists.csv.gz`
	- `song_info.csv.gz`

## Execute
```
$ make all 
```

## Just generating report from already generated output.
```
$ make report
```
