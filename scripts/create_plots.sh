for fil in `ls -1 *.csv | sed -e 's/\.csv$//'` 
do 
	R -q -e "library('igraph'); source('plotFromAdjacencyMatrix.R'); plotFromAdjacencyMatrix('$fil.csv', '$fil.png');"
done
