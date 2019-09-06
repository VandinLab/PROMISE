# ProMiSe
## Permutation Strategies for Mining Significant Sequential Patterns (Tonon and Vandin, ICDM 2019)
`ProMiSe` is an algorithm for identifying significant sequential patterns with guarantees on the probability of reporting at least one false discoveries. It employs the Westfall-Young method to correct for multiple hypothesis testing.

This code was used to test the performance and correctness of the strategy proposed in the paper. The results obtained are reported in the Experimental Evaluation section of the paper. The implementation used the PrefixSpan algorithm provided by the [SPMF library](http://www.philippe-fournier-viger.com/spmf/) to mine the dataset. The code has been developed using IntelliJ IDEA v2019.1, JAVA and the Apache Spark Framework.
We provided the source code of the parallel implementation of ProMiSe, the datasets used in the evaluation and an executable jar file to directly execute the algorithm. Note that Apache Spark is already included in the executable file.

## Usage
To execute `ProMiSe` to mine the significant sequential patterns that have frequency at least theta from a sequential dataset, you can use the [ProMiSe.jar](ProMiSe.jar) executable file in the repository. The usage is the following:

```
Usage: java -jar ProMiSe.jar dataset P T theta parallelization strategy   

Arguments:

	* 	dataset: the name of the file containing the sequential dataset. The dataset must be a .txt file in the format described in the SPMF Library documentation. It must be in the data folder. It is possible to create subdirectories in the data folder and to include them in this parameter, e.g.: java -jar ProMiSe.jar subdirectory/dataset P T theta parallelization strategy
	*	P: the number of random datasets used for the WY method (e.g. 100)
	*	T: the number of random datasets for the Monte Carlo estimate of p-values (e.g. 10048)
	*	theta: the minimum frequency threshold, in (0,1) (e.g. 0.4)
	*   	parallelization: the number of cores of the machine (e.g. 64)
	*	strategy: the strategy used by ProMiSe to generate random datasets (0 for itemsetsSwaps, 1 for permutations) (e.g. 1)

Example of usage: java -jar ProMiSe.java SIGN 100 10048 0.4 64 1

Optional VM parameters (after the java command and before the -jar):
	
	*	-XmxRG: allows to specify the maximum memory allocation pool for a Java virtual machine (JVM), where R must be replaced with an integer that represents the maximum memory in GB
```

The significant frequent sequential patterns found are saved in a file, dataset_SFSP.txt, in the data folder.
The algorithm writes in the standard output the parameters used for the execution, the corrected threshold computed by the algorithm and used to flag a sequential pattern as significant, the number of frequent sequential patterns FSP found using theta as minimum frequency threshold and the number of significant frequent sequential patterns SFSP found using theta as minimum frequency threshold and the corrected threshold for the significativity.  
  
