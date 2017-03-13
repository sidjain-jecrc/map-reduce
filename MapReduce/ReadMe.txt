How Mapper works?
Mapper takes tuples from two tables in text format as input and split each tuple line by using comma delimiter. 
Here we will extract the second column of the tuple which is the join predicate, on the basis of which we have to join the two tables. 
So, in map output we are going to set this join predicate as the key and the entire input tuple as value.

How Reducer works?
Reducer receives join predicate as key and all the tuples with the same join predicate as an iterable of values. 
So that means we will have to iterate all the tuples and check which tuple belongs to which table on the basis of table name. 
Here I have created two lists for storing tuples for each table. 
Using these two lists I have created join combinations for both the tables and stored it in the reducer output in required format.

How Driver works?
Driver is the file where we do all the configuration related to job that we want to perform. 
In this case, I have created a mapreduce job and set all the required configurations which includes setting mapper class, reducer class, output key and value classes, input and output format classes. 
As per requirement, I am reading input and output file paths from the command line. In the end, I start the job and wait for its completion.
