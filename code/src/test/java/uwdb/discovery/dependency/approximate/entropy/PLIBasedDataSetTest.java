package uwdb.discovery.dependency.approximate.entropy;

import org.junit.Test;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by gunaprsd on 5/1/17.
 */
public class PLIBasedDataSetTest {
    public static int dataSize = 10000;
    public static int numAttributes = 10;
    public static String fileName = "temp.csv";
    public static double[] expectedValuesArray;

    public PLIBasedDataSetTest()
    {
        loadData();
    }

    public static void loadData()
    {
        Random random = new Random();
        try {
            FileWriter writer = new FileWriter(fileName);
            for(int i = 0; i < dataSize; i++)
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(4);
                stringBuilder.append(", ");
                stringBuilder.append(Math.abs(i % 5));
                stringBuilder.append(", ");
                stringBuilder.append(Math.abs(i % 2));
                stringBuilder.append(", ");
                stringBuilder.append(i);
                for(int j  = 4; j < numAttributes; j++)
                {
                    stringBuilder.append(", ");
                    stringBuilder.append(Math.abs(random.nextInt() % 1000));
                }
                writer.write(stringBuilder.toString());
                writer.write("\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        expectedValuesArray = new double[4];
        expectedValuesArray[0] = 0;
        expectedValuesArray[1] = - 5 * 0.2 * Math.log(0.2);
        expectedValuesArray[2] = - 2 * 0.5 * Math.log(0.5);
        expectedValuesArray[3] = Math.log(dataSize);
    }

    @Test
    public void computeEntropy() throws Exception {
        RelationSchema schema = new RelationSchema(numAttributes);

        PLIBasedDataSet dataSet = new PLIBasedDataSet(fileName, schema, false);
        IAttributeSet subset1 = schema.getAttributeSet(0);
        assertEquals(0, dataSet.computeEntropy(subset1), 1E-9);

        IAttributeSet subset2 = schema.getAttributeSet(3);
        assertEquals(Math.log(dataSize), dataSet.computeEntropy(subset2), 1E-9);
    }


    @Test
    public void computeEntropies() throws Exception {
        RelationSchema schema = new RelationSchema(numAttributes);

        PLIBasedDataSet dataSet = new PLIBasedDataSet(fileName, schema, false);

        HashMap<IAttributeSet, Double> expectedValues = new HashMap<IAttributeSet, Double>();
        HashMap<IAttributeSet, Double> computedValues = new HashMap<IAttributeSet, Double>();

        for(int i = 0; i < 4; i++)
        {
            IAttributeSet attributeSet = schema.getAttributeSet(i);
            computedValues.put(attributeSet, Double.MAX_VALUE);

            double expectedValue = expectedValuesArray[i];
            expectedValues.put(attributeSet, expectedValue);
        }

        dataSet.computeEntropies(computedValues);
        for(IAttributeSet subset : computedValues.keySet())
        {
            assertEquals(expectedValues.get(subset), computedValues.get(subset), 1E-9);
        }
    }

}