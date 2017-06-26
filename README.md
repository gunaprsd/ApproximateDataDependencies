# Approximate Data Dependencies
We define a new notion of data dependency that is more robust to noise in datasets. Quite often, we find that the quality of functional dependencies obtained from a dataset are poor due to noise in the data. To overcome such problems we have designed a more robust notion of data dependencies that share a lot of similar properties to exact data dependencies and can be used to perform approxiate relation decompositions and size estimations, etc.

This repository contains the source code of the program that can be used on a given relational dataset to discover them directly. Further, it also consists of an inference module that identifies if a set of data dependencies imply another using just the semantics of these dependencies.
