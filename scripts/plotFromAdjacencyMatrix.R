plotFromAdjacencyMatrix <- function(adjmatcsv, figname) {
  dat = read.csv(adjmatcsv, header=FALSE)
  dat[,ncol(dat)] <- NULL
  mat = as.matrix(dat)
  net = graph.adjacency(mat, mode="undirected")
  png(figname)
  plot(net)
  dev.off()
}