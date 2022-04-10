# Comparison of Collaborative Filtering Algorithms: Weighted Bipartite Graph Projection and Alternating Least Squares

The project aimed to compare the performance of two different collaborative filtering methods in aspect of
prediction accuracy and time efficiency. The two approaches chosen in this project were the weighted bipartite
graph projection (WBGP) approach with recommendation power as the similarity measure and the matrix
factorization approach with alternating least squares (ALS) learning algorithm. The two algorithms were
implemented on a small subset of Yelp Open Dataset where the businesses were all from Washington state
and the root mean squared error (RMSE) was chosen as the evaluation metrics for the comparison. The
implementation of the algorithms was based on Apache Spark platform in Python programming language
(Pyspark). The result showed that although the alternating least squares (ALS) algorithm beat the weighted
bipartite graph projection (WBGP) in terms of computational time, the latter could produce better prediction
accuracy.
