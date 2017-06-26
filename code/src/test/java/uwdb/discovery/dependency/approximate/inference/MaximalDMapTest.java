package uwdb.discovery.dependency.approximate.inference;

import org.junit.Test;
import uwdb.discovery.dependency.approximate.Main;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.entropy.ExternalFileDataSet;
import uwdb.discovery.dependency.approximate.entropy.PLIBasedDataSet;
import uwdb.discovery.dependency.approximate.inference.bayesianmap.BayesianIMap;
import uwdb.discovery.dependency.approximate.inference.dmap.MaximalDMap;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;


public class MaximalDMapTest {
    @Test
    public void getCDEP() throws Exception {
        int numShuffles = 10;
        Main.initialize();
        String choice = "test";
        RelationSchema schema = new RelationSchema(Main.numColsDict.get(choice));
        PLIBasedDataSet dataSet = new PLIBasedDataSet(Main.filePathDict.get(choice), schema, true);
        dataSet.initialize();
        ArrayList<Integer> order = schema.getEmptyAttributeSet().complement().getIndices();
        for(int i = 0; i < numShuffles; i++) {
            PrintStream outputStream = new PrintStream(new FileOutputStream("graph" + String.valueOf(i+1) + ".csv"));
            Collections.shuffle(order);
            BayesianIMap bayesianIMap = new BayesianIMap(schema, dataSet);
            bayesianIMap.initialize();
            bayesianIMap.computeBayesianIMap(order);
            bayesianIMap.moralize();
            bayesianIMap.printMoralizedGraphAdjacencyMatrix(outputStream, "");
            outputStream.close();
        }
    }
}