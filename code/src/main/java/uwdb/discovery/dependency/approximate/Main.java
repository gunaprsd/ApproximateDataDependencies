package uwdb.discovery.dependency.approximate;
import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.entropy.PLIBasedDataSet;
import uwdb.discovery.dependency.approximate.inference.beeri.BeeriAlgorithmInference;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModuleFactory;
import uwdb.discovery.dependency.approximate.search.TopDownInductiveSearch;

import java.util.HashMap;


public class Main {

    public static HashMap<String, Integer> numColsDict;
    public static HashMap<String, String> filePathDict;

    public static void initialize()
    {
        numColsDict = new HashMap<String, Integer>();
        filePathDict = new HashMap<String, String>();

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
        filePathDict.put("flights2", baseFolder + "partial_flights2.csv");
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

    public static void main(String[] args) {
        initialize();
        performSearch("chess", 1E-6);
        //performSearch(args);
        //printLattice();
        //test(args);
    }
}
