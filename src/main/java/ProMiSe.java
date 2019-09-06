import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.*;
import java.util.*;

/**
 * This class is a parallel implementation of the algorithm ProMiSe described in
 * the paper "Permutation Strategies for Mining Significant Sequential Patterns".
 * This code was used to test the performance and correctness of the strategy proposed
 * in the paper and the results are reported in the Experimental Evaluation section
 * of the paper.
 * ProMiSe aims to extract the set of significant frequent sequential patterns (SFSP)
 * from a sequential transactional dataset, given a minimum frequency threshold theta,
 * while bounding the probability that one or more false positives are reported in output.
 * It employs the Westfall-Young method to correct for multiple hypothesis testing.
 */

public class ProMiSe implements Serializable {

    /**
     * Private class that implements a simple pair structure where the key and the value
     * are both int
     */
    private static class Pair implements Serializable{
        int x;
        int y;
        Pair(int x,int y){
            this.x=x;
            this.y=y;
        }
    }

    /**
     * Private class that implements a simple generic pair structure
     */
    private static class PairT<T1,T2> implements Serializable{
        T1 x;
        T2 y;
        PairT(T1 x,T2 y){
            this.x=x;
            this.y=y;
        }
    }

    JavaSparkContext scc;
    // input and output files
    String fileIn;
    String fileOut;
    // data structures to store the datasets data
    Int2ObjectOpenHashMap<Pair> positionOrigin;
    ObjectArrayList<IntArrayList> datasetOrigin;
    Object2IntOpenHashMap<String> itemsetsOrigin;
    Int2ObjectOpenHashMap<String> itemsetsReverseOrigin;
    ObjectArrayList<IntArrayList> datasetRandom;

    /**
     * Constructor
     * @param   fileIn  file of the input dataset to mine the SFSP
     * @param   fileOut file to store the SFSP
     * @param   scc     JavaSparkContext for the parallel execution
     */
    ProMiSe(String fileIn, String fileOut, JavaSparkContext scc){
        this.fileIn = fileIn;
        this.fileOut = fileOut;
        this.scc = scc;
        positionOrigin = new Int2ObjectOpenHashMap<>();
        datasetOrigin = new ObjectArrayList<>();
        itemsetsOrigin = new Object2IntOpenHashMap<>();
        itemsetsReverseOrigin = new Int2ObjectOpenHashMap<>();
    }

    /**
     * Computes the p-values in parallel with a Monte Carlo procedure. It returns the p-values computed
     * by this core.
     * @param   index           the index of the core used
     * @param   fileIn          the file of the actual dataset of which we want to compute the p-values
     * @param   fileMined       the file that contains the FSP mined from the actual dataset
     * @param   strategy        the strategy used to generate random datasets (0 for itemsetsSwaps,
     *                          1 for permutations)
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   parallelization the number of cores of the machine
     * @param   theta           the minimum frequency threshold used to mine the datasets
     * @param   seed            the seed used for this dataset for the random generator
     *                          (fixed for reproducibility)
     * @return                  the p-values computed by this core
     */
    private static int[] parallelComputePValues(int index, String fileIn, String fileMined, int strategy, int T, int parallelization, double theta, int seed) throws IOException {
        String file = fileIn.split("\\.")[0];
        String fileRandom = file + "_random_" +index+ ".txt";
        String fileMinned = file + "_random_" +index+ "_mined.txt";
        // data structures to store the actual dataset and the FSP mined from it
        Int2ObjectOpenHashMap<Pair> position = new Int2ObjectOpenHashMap<>();
        ObjectArrayList<IntArrayList> dataset = new ObjectArrayList<>();
        Object2IntOpenHashMap<String> itemsets = new Object2IntOpenHashMap<>();
        Int2ObjectOpenHashMap<String> itemsetsReverse = new Int2ObjectOpenHashMap<>();
        Object2IntOpenHashMap<String> spSupp = new Object2IntOpenHashMap<>();
        Object2IntOpenHashMap<String> spIndex = new Object2IntOpenHashMap<>();
        // fixes the seed of the random generator for reproducibility
        Random r = new Random(seed+index);
        // loads the actual dataset in the data structures provided in input
        loadDataset(fileIn,position,dataset,itemsets,itemsetsReverse);
        // reads the FSP mined from the actual dataset and stores them in the data structures
        int numSP = readOut(fileMined,spSupp,spIndex);
        int[] pValue = new int[numSP];
        // generates T/parallelization random datasets with this core
        for(int j=0;j<T/parallelization;j++){
            switch (strategy){
                // uses 2*m itemsets swaps to generate a random dataset
                case 0: itemsetsSwaps(2*position.size(),r,position,dataset);
                    break;
                case 1: permutation(r,dataset);
                    break;
            }
            // writes the random dataset generated
            writeDataset(fileRandom,dataset,itemsetsReverse);
            // mines the random dataset generated
            mining(fileRandom,fileMinned,theta);
            // updates the p-values with the FSP of the random dataset generated
            updatePValue(fileMinned,spSupp,pValue,spIndex);
        }
        // deletes the files generated
        File fileR = new File(fileRandom);
        fileR.delete();
        File fileM = new File(fileMinned);
        fileM.delete();
        return pValue;
    }

    /**
     * Reads the dataset from FileIn file and stores the dataset in the data structures provided in input.
     * @param   fileIn          the name of the file that contains the dataset to load
     * @param   positions       an hashmap where it assigns an index to each position of the dataset
     * @param   dataset         an arraylist where it stores the index of the itemsets in the transactions of the dataset.
     *                          Each element of the arraylist represents a transactions and it is an arraylist of
     *                          integers. The integers in such arraylist are the indexes of the itemsets in that transaction.
     * @param   itemsets        an hashmap where it assigns to each itemset in the dataset an integer index
     * @param   itemsetsReverse an hashmap where it stores the reverse index of the previous hashmap
     */
    private static void loadDataset(String fileIn, Int2ObjectOpenHashMap<Pair> positions, ObjectArrayList<IntArrayList> dataset,Object2IntOpenHashMap<String> itemsets, Int2ObjectOpenHashMap<String> itemsetsReverse) throws IOException {
        FileReader fr = new FileReader(fileIn);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        int i = 0;
        int itemset = 0;
        int position = 0;
        while(line!=null){
            IntArrayList transaction = new IntArrayList();
            String[] splitted = line.split(" -1 ");
            for(int j=0;j<splitted.length-1;j++){
                int currItemset;
                if(!itemsets.containsKey(splitted[j])){
                    currItemset = itemset;
                    itemsets.put(splitted[j],itemset);
                    itemsetsReverse.put(itemset++,splitted[j]);
                }
                else currItemset = itemsets.getInt(splitted[j]);
                positions.put(position++,new Pair(i,j));
                transaction.add(currItemset);
            }
            dataset.add(transaction);
            line = br.readLine();
            i++;
        }
        br.close();
        fr.close();
    }

    /**
     * Mines the dataset in the fileIn file using theta as minimum
     * frequency threshold and stores in fileOut the FSP found.
     * This method uses the PrefixSpan implementation provided in the SPMF library.
     * @param   fileIn   the name of the file that contains the dataset to mine
     * @param   fileFSP  the name of the file where the method stores the sequential patterns found
     * @param   theta    the minimum frequency threshold used to mine the dataset
     */
    private static void mining(String fileIn, String fileFSP, double theta) throws IOException{
        AlgoPrefixSpan alg = new AlgoPrefixSpan();
        alg.runAlgorithm(fileIn,theta,fileFSP);
    }

    /**
     * Reads the file that contains the FSP mined from a dataset and stores them in two hashmaps.
     * It returns the number of FSP read.
     * @param   fileMined   the name of the file to read that contains the FSP mined from a dataset
     * @param   spSupp      an hashmap to store the FSP and their supports. The hashmap has strings that represent
     *                      the sequential patterns as keys and integers that represent the supports of the sequential
     *                      patterns as values
     * @param   spIndex     an hashmap to assign to each sequential pattern an integer index. The hashmap has strings
     *                      that represent the sequential patterns as keys and integers that represent the indexes
     *                      assigned to the sequential patterns as values. This hashmap is used to speed up the
     *                      computation of the p-values
     * @return              the number of the FSP read from the file
     */
    private static int readOut(String fileMined, Object2IntOpenHashMap<String> spSupp, Object2IntOpenHashMap<String> spIndex) throws IOException {
        FileReader fr = new FileReader(fileMined);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        int index = 0;
        while(line!=null){
            String[] splitted = line.split(" #SUP: ");
            spSupp.put(splitted[0],Integer.parseInt(splitted[1]));
            spIndex.put(splitted[0],index++);
            line = br.readLine();
        }
        br.close();
        fr.close();
        return spIndex.size();
    }

    /**
     * It generates a random dataset performing a series of numberOfSwaps itemsetsSwaps.
     * It returns the time required for the generation.
     * @param   numberOfSwaps   the number of itemsetsSwaps to perform
     * @param   rand            the random generator used to choose the swaps
     * @param   position        the hashmap that contains the positions of the dataset (See loadDataset method)
     * @param   dataset         the hashmap that contains the indexes of the itemsets in the dataset (See loadDataset method).
     *                          This hashmap contains the random dataset after the execution
     * @return                  the time required to generate the random dataset
     */
    private static long itemsetsSwaps(int numberOfSwaps, Random rand, Int2ObjectOpenHashMap<Pair> position, ObjectArrayList<IntArrayList> dataset){
        long start = System.currentTimeMillis();
        for(int i=0;i<numberOfSwaps;i++) {
            Pair p1 = position.get(rand.nextInt(position.size()));
            Pair p2 = position.get(rand.nextInt(position.size()));
            int i1 = dataset.get(p1.x).getInt(p1.y);
            int i2 = dataset.get(p2.x).getInt(p2.y);
            dataset.get(p1.x).set(p1.y, i2);
            dataset.get(p2.x).set(p2.y, i1);
        }
        return System.currentTimeMillis()-start;
    }

    /**
     * It generates a random dataset permuting the transactions of the dataset.
     * It returns the time required for the generation.
     * @param   rand    the random generator to permute the transaction
     * @param   dataset the hashmap that contains the indexes of the itemsets in the dataset (See loadDataset method).
     *                  This hashmap contains the random dataset after the execution
     * @return          the time required to generate the random dataset
     */
    private static long permutation(Random rand, ObjectArrayList<IntArrayList> dataset){
        long start = System.currentTimeMillis();
        for(int i=0;i<dataset.size();i++) {
            Collections.shuffle(dataset.get(i),rand);

        }
        return System.currentTimeMillis()-start;
    }

    /**
     * Updates the data for the p-values computation after the mining of a new random dataset.
     * @param   file    the name of the file with the FSP mined from the random dataset
     * @param   spSupp  the hashmap that contains the supports of the FSP mined from the starting dataset
     * @param   pValue  the array containing the data for the p-values computation
     * @param   spIndex the hashmap that contains the indexes of the FSP mined from the starting dataset
     */
    private static void updatePValue(String file, Object2IntOpenHashMap<String> spSupp, int[] pValue, Object2IntOpenHashMap<String> spIndex) throws IOException{
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while(line!=null){
            String[] splitted = line.split(" #SUP: ");
            if(spSupp.containsKey(splitted[0])){
                int suppO = spSupp.getInt(splitted[0]);
                int suppS = Integer.parseInt(splitted[1]);
                if(suppO <= suppS) pValue[spIndex.getInt(splitted[0])]++;
            }
            line = br.readLine();
        }
        br.close();
        fr.close();
    }

    /**
     * Writes a random dataset in the FileOut file.
     * @param   fileOut         the name of the file where it writes the dataset
     * @param   dataset         the hashmap that contains the indexes of the itemsets in the dataset (See loadDataset method)
     * @param   itemsetsReverse the hashmap that contains the indexes and the corresponding itemsets
     */
    private static void writeDataset(String fileOut, ObjectArrayList<IntArrayList> dataset, Int2ObjectOpenHashMap<String> itemsetsReverse) throws IOException {
        FileWriter fw = new FileWriter(fileOut);
        BufferedWriter bw = new BufferedWriter(fw);
        for(int i=0;i<dataset.size();i++){
            IntArrayList transaction = dataset.get(i);
            String line = "";
            for(int j=0;j<transaction.size();j++){
                line+=itemsetsReverse.get(transaction.getInt(j))+" -1 ";
            }
            line+="-2\n";
            bw.write(line);
        }
        bw.close();
        fw.close();
    }

    /**
     * Compute the p-values of the FSP of a dataset merging the p-values computed by the different cores.
     * @param   index           the index of the actual dataset of the WY method (used to fix
     *                          the seed of the random generator for reproducibility)
     * @param   P               the number of random datasets used for the WY method
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   parallelization the number of cores of the machine. It is the number of random datasets
     *                          generated at the same time in the Monte Carlo estimation. Each core of
     *                          the machine computes the p-values on T/parallelization random datasets.
     * @param   file            the file of the dataset of which we want to compute the p-values
     * @param   fileMined       the file containing the FSP extracted from the dataset stored in file
     * @param   strategy        the strategy used by ProMiSe to generate random datasets (0 for
     *                          itemsetsSwaps, 1 for permutations)
     * @param   theta           the minimum frequency threshold used to mine the dataset
     * @return
     */
    private int[] computePValues(int index, int P, int T, int parallelization, String file, String fileMined, int strategy, double theta){
        // the indexes of the cores of the machine
        IntArrayList indexes = new IntArrayList();
        for (int i = 0; i < parallelization; i++) indexes.add(i+1);
        // fixes the seed for reproducibility
        int seed = P*(index+1);
        // computes in parallel the p-values from different random datasets
        List<int[]> result = scc.parallelize(indexes, parallelization).map(o1 -> parallelComputePValues(o1, file, fileMined, strategy, T, parallelization,theta,seed)).collect();
        // merges the p-values computed by the different cores
        int[] pValueInt = new int[result.get(0).length];
        for (int[] curr : result) {
            for (int k = 0; k < curr.length; k++) pValueInt[k] += curr[k];
        }
        return pValueInt;
    }

    /**
     * Executes ProMiSe algorithm. It reads the starting dataset from file and after the computation
     * it stored the SFSF found in teh output file.
     * @param   P               the number of random datasets used for the WY method
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   parallelization the number of cores of the machine. It is the number of random datasets
     *                          generated at the same time in the Monte Carlo estimation. Each core of
     *                          the machine computes the p-values on T/parallelization random datasets.
     * @param theta             the minimum frequency threshold used to mine the dataset
     * @param strategy          the strategy used by ProMiSe to generate random datasets (0 for
     *                          itemsetsSwaps, 1 for permutations)
     */
    void execute(int P, int T, int parallelization, double theta, int strategy) throws IOException {
        String file = fileIn.split("\\.")[0];
        String fileRandom = file + "_random.txt";
        String fileMinned = file + "_mined.txt";
        // loads the input dataset in the data structures provided in input
        loadDataset(fileIn,positionOrigin,datasetOrigin,itemsetsOrigin,itemsetsReverseOrigin);
        // fixes the seed for the first random generator
        Random r = new Random(0);
        double[] minPvalue = new double[P];
        // for all the P datasets of the WY method
        for(int j=0;j<P;j++) {
            datasetRandom = datasetOrigin.clone();
            switch (strategy) {
                case 0:
                    // uses 2*m itemsets swaps to generate a random dataset
                    itemsetsSwaps(2*positionOrigin.size(), r, positionOrigin, datasetRandom);
                    break;
                case 1:
                    permutation(r, datasetRandom);
                    break;
            }
            // writes the actual random dataset generated
            writeDataset(fileRandom,datasetRandom,itemsetsReverseOrigin);
            // mines the actual random dataset generated
            mining(fileRandom, fileMinned, theta);
            // computes in parallel the p-values of the actual random dataset
            int[] pValueInt = computePValues(j,P,T,parallelization,fileRandom,fileMinned,strategy,theta);
            // finds the minimum of the p-values computed from the actual random dataset
            int min = Integer.MAX_VALUE;
            for(int k=0;k<pValueInt.length;k++) {
                if(min>pValueInt[k]) min = pValueInt[k];
            }
            minPvalue[j] = (1 + min) / (T * 1. + 1);
        }
        // sorts the P minimum p-values
        Arrays.sort(minPvalue);
        // computes the corrected threshold
        double correctedThreshold = minPvalue[(int)(P*0.05)];
        System.out.println("Corrected Threshold: " + correctedThreshold);
        // if the corrected threshold is greater than the minimum possible value
        if(correctedThreshold!=1/(T * 1. + 1)){
            // mines the starting dataset
            mining(fileIn, fileMinned, theta);
            file = fileIn;
            // computes the p-values of the starting dataset in parallel
            int[] pValueInt = computePValues(P,P,T,parallelization,file,fileMinned,strategy,theta);
            // reads and stores the FSP mined from the starting dataset
            FileReader fr = new FileReader(fileMinned);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            Object2IntOpenHashMap<String> fspSup = new Object2IntOpenHashMap<>();
            ObjectArrayList<String> fsp = new ObjectArrayList<>();
            while (line != null) {
                String[] splitted = line.split(" #SUP: ");
                fsp.add(splitted[0]);
                fspSup.put(splitted[0], Integer.parseInt(splitted[1]));
                line = br.readLine();
            }
            br.close();
            fr.close();
            System.out.println("# FSP: " + fsp.size());
            ObjectArrayList<PairT<String, Double>> sfsp = new ObjectArrayList<>();
            for (int k = 0; k < pValueInt.length; k++) {
                sfsp.add(new PairT(fsp.get(k), (1 + pValueInt[k]) / (T * 1. + 1)));
            }
            // sort the sfsp
            sfsp.sort(Comparator.comparing(o -> o.y));
            // writes the sfsp in the output file with their support and p-value
            FileWriter fw = new FileWriter(fileOut);
            BufferedWriter bw = new BufferedWriter(fw);
            int numSFSP = 0;
            for (PairT<String, Double> currPair : sfsp) {
                // if the p-value is lower than the corrected threshold
                if(currPair.y<correctedThreshold){
                    numSFSP++;
                    bw.write(currPair.x + " #SUP: " + fspSup.getInt(currPair.x) + " #P-VALUE: " + currPair.y + "\n");
                }
            }
            bw.close();
            fw.close();
            System.out.println("# SFSP: " + numSFSP);
        }
        else System.out.println("# SFSP: No SFSP Found!");
        // deletes the files generated
        File fileR = new File(fileRandom);
        fileR.delete();
        File fileM = new File(fileMinned);
        fileM.delete();
    }

    /**
     * Main class of ProMiSe:
     * Input parameters:
     * 0:   dataset name. A dataset in SPMF format, es: FIFA,BIBLE,SIGN,BIKE,LEVIATHAN.
     *      It must be in the data folder. It is possible to include subfolders es:
     *      subfolder1/subfolder2/dataset.
     * 1:   the number of random datasets used for the WY method
     * 2:   the number of random datasets for the Monte Carlo estimate of p-values
     * 3:   the minimum frequency threshold
     * 4:   the number of cores of the machine
     * 5:   the strategy used by ProMiSe to generate random datasets (0 for
     *      itemsetsSwaps, 1 for permutations)
     */
    public static void main(String[] args) throws IOException {
        String dataset = args[0];
        int P = Integer.parseInt(args[1]);
        int T = Integer.parseInt(args[2]);
        double theta = Double.parseDouble(args[3]);
        int parallelization = Integer.parseInt(args[4]);
        int strategy = Integer.parseInt(args[5]);
        String fileIn = "data/"+dataset+".txt";
        String fileOut = "data/"+dataset+"_SFSP.txt";
        System.out.println("Dataset: " + dataset);
        System.out.println("P: " + P);
        System.out.println("T: " + T);
        System.out.println("Theta: " + theta);
        System.out.println("Parallelization: " + parallelization);
        System.out.println("Strategy: " + ((strategy==0)?"Itemsets Swaps":"Permutations"));
        long start = System.currentTimeMillis();
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("ProMiSe").set("spark.executor.memory","5g").set("spark.driver.memory","5g").set("spark.executor.heartbeatInterval","10000000").set("spark.network.timeout", "10000000");
        JavaSparkContext scc = new JavaSparkContext(sparkConf);
        ProMiSe prom = new ProMiSe(fileIn,fileOut,scc);
        prom.execute(P,T,parallelization,theta,strategy);
        scc.stop();
        System.out.println("Total Execution Time: " + (System.currentTimeMillis()-start) + " ms");
    }
}