package uwdb.discovery.dependency.approximate.entropy;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

import java.util.*;

public class InMemoryDataSet extends AbstractDataSet {
    protected RelationSchema schema;
    protected int numAttributes;
    protected long[][] data;

    public InMemoryDataSet(String fileName, RelationSchema schema)
    {
        super(schema);
        this.numAttributes = schema.getNumAttributes();
    }
    public InMemoryDataSet(long[][] data, RelationSchema schema)
    {
        super(schema);
        this.numAttributes = schema.getNumAttributes();
        this.data = data;
    }

    public RelationSchema getSchema() {
        return schema;
    }

    public void initialize() {

    }

    public double computeEntropy(IAttributeSet subset) {
        HashMap<Integer, Long> counts = new HashMap<Integer, Long>();
        for(int i = 0; i < data.length; i++)
        {
            addCount(counts, data[i], subset);
        }
        return computeEntropyValue(counts, data.length);
    }

    public void computeEntropies(HashMap<IAttributeSet, Double> values) {
        HashMap<IAttributeSet, HashMap<Integer, Long>> counts = new HashMap<IAttributeSet, HashMap<Integer, Long>>();
        for(IAttributeSet attributeSet : values.keySet()) {
            counts.put(attributeSet, new HashMap<Integer, Long>());
        }

        for(int i = 0; i < data.length; i++)
        {
            for (IAttributeSet attributeSet : values.keySet())
            {
                addCount(counts.get(attributeSet), data[i], attributeSet);
            }
        }

        for (IAttributeSet attributeSet : values.keySet()) {
            HashMap<Integer, Long> countsMap = counts.get(attributeSet);
            double entropyValue = computeEntropyValue(countsMap, data.length);
            values.put(attributeSet, entropyValue);
        }
    }
}
