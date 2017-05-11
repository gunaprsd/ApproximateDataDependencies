package uwdb.discovery.dependency.approximate.entropy;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

import java.io.*;
import java.util.*;

public class ExternalFileDataSet extends AbstractDataSet {
    protected boolean hasHeader;
    protected String fileName;
    protected BufferedReader reader;

    public ExternalFileDataSet(String fileName, RelationSchema schema) {
        this(fileName, schema, false);
    }

    public ExternalFileDataSet(String fileName, RelationSchema schema, boolean hasHeader) {
        super(schema);
        this.fileName = fileName;
        this.hasHeader = hasHeader;
        try {
            this.reader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public RelationSchema getSchema() {
        return schema;
    }

    public void initialize() {

    }

    public double computeEntropy(IAttributeSet subset) {
        long numLines = 0;
        String[] tempData = new String[numAttributes];
        String line;
        HashMap<Integer, Long> counts = new HashMap<Integer, Long>();
        try {
            reader = new BufferedReader(new FileReader(fileName));
            if(hasHeader)
            {
                line = reader.readLine();
            }
            while((line  = reader.readLine()) != null)
            {
                parseLine(line, tempData);
                addCount(counts, tempData, subset);
                numLines += 1;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return computeEntropyValue(counts, numLines);
    }

    protected void parseLine(String line, String[] destinationArray)
    {
        String[] parts = line.split(",|;");
        for(int i = 0; i < numAttributes; i++) {
            destinationArray[i] = parts[i].trim();
        }
    }

    public void computeEntropies(HashMap<IAttributeSet, Double> values) {
        long numLines = 0;
        String[] lineData = new String[numAttributes];
        String line;

        HashMap<IAttributeSet, HashMap<Integer, Long>> counts = new HashMap<IAttributeSet, HashMap<Integer, Long>>();
        for(IAttributeSet attributeSet : values.keySet()) {
            counts.put(attributeSet, new HashMap<Integer, Long>());
        }

        try {
            reader = new BufferedReader(new FileReader(fileName));
            while((line  = reader.readLine()) != null) {
                parseLine(line, lineData);
                for (IAttributeSet attributeSet : values.keySet()) {
                    addCount(counts.get(attributeSet), lineData, attributeSet);
                }
                numLines += 1;

                if(numLines % 10000 == 0) {
                    //System.out.printf("Read %d lines\n", numLines);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        for (IAttributeSet attributeSet : values.keySet()) {
            HashMap<Integer, Long> countsMap = counts.get(attributeSet);
            double entropyValue = computeEntropyValue(countsMap, numLines);
            values.put(attributeSet, entropyValue);
        }

        if(numRows == 0) {
            numRows = numLines;
        }
    }
}
