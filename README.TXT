YET ANOTHER TPCW (and others) BENCHMARK
=========================

#Propose:
YCBS isn't out yet, and it misses some things I think. This is an adaptable benchmark tool that already includes master slave coordination, probability distributions, and a Cassandra interface (some problems to be resolved).

#About: 
I'm no database expert, and I have full knowledge that benchmarking isn't simple. But it's in the moment, a pretty naive approach to evaluate the risks of using an eventual consistency database in a e-commerce environment 

#Use:
	-cb - Benchmark cleaning, removes the basic tables needed for execution, and can introduce fresh values. 
	-c - Full Database cleaning

	-p - Populates the database
	
	-m - Execute in master mode.
	
	-s - Execute in slave mode. 

The configuration files: 
	TODO-  	 

#Other:
 - The dataviewer folder refers to a small swing browsing application for Cassandra, but I never change it to support Cassandra5+, and it doesn't show Super Columns. I think there are other options out there.

- Configuration files and libs are missing, I will take care of that soon I hope 	