package uwdb.discovery.dependency.approximate.entropy;

import org.junit.Test;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.*;

public class InMemoryDataSetTest {
    public static int dataSize = 100;
    public static int numAttributes = 4;
    public static long[][] data;
    public static double[] expectedValuesArray;

    public static void loadData()
    {
        Random random = new Random();
        data = new long[dataSize][numAttributes];
        for(int i = 0; i < dataSize; i++)
        {
            data[i][0] = 4;
            data[i][1] = Math.abs(i % 5);
            data[i][2] = Math.abs(i % 2);
            data[i][3] = i;
            for(int j  = 4; j < numAttributes; j++)
            {
                data[i][j] = Math.abs(random.nextInt() % 1000);
            }
        }

        expectedValuesArray = new double[4];
        expectedValuesArray[0] = 0;
        expectedValuesArray[1] = - 5 * 0.2 * Math.log(0.2);
        expectedValuesArray[2] = - 2 * 0.5 * Math.log(0.5);
        expectedValuesArray[3] = Math.log(dataSize);

    }

    public InMemoryDataSetTest()
    {
        loadData();
    }

    @Test
    public void computeEntropy() throws Exception {
        RelationSchema schema = new RelationSchema(numAttributes);

        InMemoryDataSet dataSet = new InMemoryDataSet(data, schema);
        IAttributeSet subset1 = schema.getAttributeSet(0);
        assertEquals(0, dataSet.computeEntropy(subset1), 1E-9);

        IAttributeSet subset2 = schema.getAttributeSet(3);
        assertEquals(Math.log(dataSize), dataSet.computeEntropy(subset2), 1E-9);
    }

    @Test
    public void computeEntropies() throws Exception {
        RelationSchema schema = new RelationSchema(numAttributes);

        InMemoryDataSet dataSet = new InMemoryDataSet(data, schema);

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