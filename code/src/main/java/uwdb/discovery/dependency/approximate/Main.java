package uwdb.discovery.dependency.approximate;
import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.entropy.PLIBasedDataSet;
import uwdb.discovery.dependency.approximate.inference.bayesianmap.BayesianIMap;
import uwdb.discovery.dependency.approximate.inference.beeri.BeeriAlgorithmInference;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDataset;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModuleFactory;
import uwdb.discovery.dependency.approximate.search.TopDownInductiveSearch;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;


public class Main {

    public static HashMap<String, Integer> numColsDict;
    public static HashMap<String, String> filePathDict;

    public static void initialize() {
        numColsDict = new HashMap<>();
        filePathDict = new HashMap<>();

        String baseFolder = "/Users/gunaprsd/Source/data/";

        numColsDict.put("abalone", 9);
        filePathDict.put("abalone", baseFolder + "abalone.csv");

        numColsDict.put("balance-scale", 5);
        filePathDict.put("balance-scale", baseFolder + "balance-scale.csv");

        numColsDict.put("chess", 7);
        filePathDict.put("chess", baseFolder + "chess.csv");

        numColsDict.put("iris", 5);
        filePathDict.put("iris", baseFolder + "iris.csv");

        numColsDict.put("nursery", 9);
        filePathDict.put("nursery", baseFolder + "nursery.csv");

        numColsDict.put("hepatitis", 18);
        filePathDict.put("hepatitis", baseFolder + "hepatitis.csv");

        numColsDict.put("horse", 27);
        filePathDict.put("horse", baseFolder + "horse.csv");

        numColsDict.put("flights", 26);
        filePathDict.put("flights", baseFolder + "partial_flights.csv");

        numColsDict.put("flights2", 11);
        filePathDict.put("flights2", baseFolder + "partial_flights3.csv");

        numColsDict.put("test", 5);
        filePathDict.put("test", baseFolder + "test.csv");
    }

    public static void performSearch(String filePath, int numCols, double alpha) {
        RelationSchema schema = new RelationSchema(numCols);
        PLIBasedDataSet dataSet = new PLIBasedDataSet(filePath, schema, true);
        IInferenceModuleFactory inferenceModuleFactory = new BeeriAlgorithmInference.Factory(schema, alpha);
        TopDownInductiveSearch search = new TopDownInductiveSearch(DependencyType.FUNCTIONAL_DEPENDENCY, dataSet, inferenceModuleFactory, alpha);
        search.search();
        search.printDiscoveredDependencies();
        search.printRuntimeCharacteristics();
        dataSet.printAccessesCount();
    }

    public static void performSearch(String choice, double alpha) {
        String filePath;
        int numCols;

        if(!numColsDict.containsKey(choice)) {
            System.out.println("Unknown dataset");
            System.exit(0);
        }
        numCols = numColsDict.get(choice);
        filePath = filePathDict.get(choice);
        alpha = 1E-6;

        performSearch(filePath, numCols, alpha);
    }

    public static void performSearch(String[] args) {
        String filePath;
        int numCols;
        double alpha;

        filePath = args[0];
        numCols = Integer.valueOf(args[1]);
        if(args.length > 2) {
            alpha = Double.valueOf(args[2]);
        } else {
            alpha = 1E-6;
        }

        performSearch(filePath, numCols, alpha);
    }

    protected static ArrayList<Integer> getIncreasingEntropyOrder(RelationSchema schema, IDataset dataset) {
        HashMap<IAttributeSet, Double> entropyValues = new HashMap<>();
        for(int i = 0; i < schema.getNumAttributes(); i++) {
            entropyValues.put(schema.getAttributeSet(i), Double.MAX_VALUE);
        }
        dataset.computeEntropies(entropyValues);
        ArrayList<Map.Entry<IAttributeSet, Double>> list = new ArrayList<>(entropyValues.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<IAttributeSet, Double>>() {
            @Override
            public int compare(Map.Entry<IAttributeSet, Double> o1, Map.Entry<IAttributeSet, Double> o2) {
                return o2.getValue() - o1.getValue() < 0 ? -1 : 1;
            }
        });

        ArrayList<Integer> order = new ArrayList<>();
        for(Map.Entry<IAttributeSet, Double> entry : list) {
            order.add(entry.getKey().nextAttribute(0));
        }
        return order;
    }

    public static void findMarkovModelIMaps(String choice) {
        int numShuffles = 50;
        RelationSchema schema = new RelationSchema(Main.numColsDict.get(choice));
        PLIBasedDataSet dataSet = new PLIBasedDataSet(Main.filePathDict.get(choice), schema, true);
        dataSet.initialize();
        ArrayList<Integer> order = schema.getEmptyAttributeSet().complement().getIndices();
        String baseFolder = "/Users/gunaprsd/Source/graph_plots/";
        for(int i = 0; i < numShuffles; i++) {
            PrintStream outputStream = null;
            try {
                String id = "graph" + String.valueOf(i+1);
                outputStream = new PrintStream(new FileOutputStream(baseFolder + id + ".csv"));
                Collections.shuffle(order);
                BayesianIMap bayesianIMap = new BayesianIMap(schema, dataSet);
                bayesianIMap.initialize();
                bayesianIMap.computeBayesianIMap(order);
                bayesianIMap.moralize();
                bayesianIMap.printMoralizedGraphAdjacencyMatrix(outputStream, id);
                outputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static void findMarkovModelIMapIncreasingEntropyOrder(String choice) {
        RelationSchema schema = new RelationSchema(Main.numColsDict.get(choice));
        PLIBasedDataSet dataSet = new PLIBasedDataSet(Main.filePathDict.get(choice), schema, true);
        dataSet.initialize();
        ArrayList<Integer> order = new ArrayList<>();
        order.add(0);
        order.add(1);
        order.add(3);
        order.add(2);
        order.add(4);
        order.add(5);
        order.add(6);
        order.add(7);
        order.add(8);
        order.add(9);
        order.add(10);
        String baseFolder = "/Users/gunaprsd/Source/graph_plots/";
        PrintStream outputStream = null;
        try {
            String id = "graph";
            outputStream = new PrintStream(new FileOutputStream(baseFolder + id + ".csv"));
            BayesianIMap bayesianIMap = new BayesianIMap(schema, dataSet);
            bayesianIMap.initialize();
            bayesianIMap.computeBayesianIMap(order);
            //bayesianIMap.computeBayesianIMapGreedyOrdering();
            bayesianIMap.moralize();
            bayesianIMap.print();
            bayesianIMap.printMoralizedGraphAdjacencyMatrix(outputStream, id);
            outputStream.close();
            bayesianIMap.findCliques();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        initialize();
        //findMarkovModelIMaps("flights2");
        findMarkovModelIMapIncreasingEntropyOrder("flights2");
        //performSearch("test", 1E-6);
        //performSearch(args);
        //printLattice();
        //test(args);
    }
    
}
