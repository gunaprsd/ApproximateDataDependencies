package uwdb.discovery.dependency.approximate.entropy;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.PositionListIndex;
import uwdb.discovery.dependency.approximate.common.RelationSchema;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.logging.*;

public class PLIBasedDataSet extends AbstractDataSet {

    private final static Logger LOGGER = Logger.getLogger(PLIBasedDataSet.class.getName());;
    private static FileHandler fileHandler = null;

    public static void loggerInitialize() {
        try {
            fileHandler = new FileHandler("DatasetLogging.log", false);
            fileHandler.setFormatter(new SimpleFormatter());
            LOGGER.setUseParentHandlers(false);
            LOGGER.addHandler(fileHandler);
            LOGGER.setLevel(Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class AttributeSetData
    {
        public PositionListIndex pli;
        public double entropy;
        public boolean entropyComputed;

        public AttributeSetData(PositionListIndex pli) {
            this.pli = pli;
            this.entropy = -1;
            this.entropyComputed = false;
        }

        public AttributeSetData(PositionListIndex pli, double entropy) {
            this.pli = pli;
            this.entropy = entropy;
            this.entropyComputed = true;
        }

        public double getEntropy(long totalNumRows) {
            if(!entropyComputed) {
                List<LongArrayList> clusters = pli.getClusters();
                entropy = Math.log(totalNumRows);
                double removeEntropy =0.0;
                for(LongArrayList cluster : clusters) {
                    long size = cluster.size();
                    removeEntropy += size * Math.log(size);
                }
                removeEntropy /= totalNumRows;
                entropy -= removeEntropy;
                entropyComputed = true;
            }
            return entropy;
        }
    }

    protected String headerLine;
    protected boolean hasHeader;
    protected String fileName;
    protected BufferedReader reader;
    protected HashMap<Integer, PositionListIndex> basePLIs;
    protected HashMap<IAttributeSet, PositionListIndex> pliCache;
    protected HashMap<IAttributeSet, Double> entropyCache;
    protected HashMap<IAttributeSet, Long> entropyAccessCount;

    public PLIBasedDataSet(String fileName, RelationSchema schema, boolean hasHeader) {
        super(schema);
        this.fileName = fileName;
        this.hasHeader = hasHeader;
        this.entropyCache = new HashMap<IAttributeSet, Double>();
        this.entropyAccessCount = new HashMap<IAttributeSet, Long>();
        this.pliCache = new HashMap<IAttributeSet, PositionListIndex>();
        loggerInitialize();
    }

    public RelationSchema getSchema() {
        return schema;
    }

    public void initialize() {
        try {
            reader = new BufferedReader(new FileReader(fileName));
            computeBasePLIs(reader);
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(Map.Entry<IAttributeSet, Double> entry : entropyCache.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        preComputeEntropies();
    }

    public void readHeader(BufferedReader reader) throws IOException {
        if(hasHeader) {
            headerLine = reader.readLine();
        }
    }

    public void computeBasePLIs(BufferedReader reader) throws IOException {
        //temporary storage for unpurged position list indices for each attribute
        ArrayList<HashMap<String, LongArrayList>> unpurgedPLIs = new ArrayList<HashMap<String, LongArrayList>>();
        for(int i = 0; i < numAttributes; i++) {
            unpurgedPLIs.add(new HashMap<String, LongArrayList>());
        }

        //read line by line and create pli for all attributes
        int tupleId = 1;
        String line = null;
        String[] tempData = new String[numAttributes];
        readHeader(reader);
        while((line  = reader.readLine()) != null) {
            parseLine(line, tempData);
            for(int i = 0; i < tempData.length; i++) {
                HashMap<String, LongArrayList> pli = unpurgedPLIs.get(i);
                if(pli.containsKey(tempData[i])) {
                    pli.get(tempData[i]).add(tupleId);
                } else {
                    LongArrayList cluster = new LongArrayList();
                    cluster.add(tupleId);
                    pli.put(tempData[i], cluster);
                }
            }
            tupleId++;
        }
        numRows = (tupleId - 1);

        //purge the plis to create smaller ones
        basePLIs = new HashMap<Integer, PositionListIndex>();
        for(int i = 0; i < numAttributes; i++) {
            HashMap<String, LongArrayList> unpurgedList = unpurgedPLIs.get(i);

            //remove all 1 size clusters
            ArrayList<LongArrayList> clusters = new ArrayList<LongArrayList>();
            for(Map.Entry<String, LongArrayList> entry: unpurgedList.entrySet()) {
                if(entry.getValue().size() > 1) {
                    clusters.add(entry.getValue());
                }
            }

            IAttributeSet attributeSet = schema.getAttributeSet(i);
            PositionListIndex pli = new PositionListIndex(clusters);
            basePLIs.put(i, pli);
            entropyCache.put(attributeSet, entropyComputer(pli));
            entropyAccessCount.put(attributeSet, 0L);
        }
    }

    protected double entropyComputer(PositionListIndex pli) {
        List<LongArrayList> clusters = pli.getClusters();
        double entropy = Math.log(numRows);
        double removeEntropy =0.0;
        for(LongArrayList cluster : clusters) {
            long size = cluster.size();
            removeEntropy += size * Math.log(size);
        }
        removeEntropy /= numRows;
        entropy -= removeEntropy;
        return entropy;
    }

    public void preComputeEntropies()
    {

        for(int i = 0; i < numAttributes; i++) {
            preComputeNextLevelEntropies(schema.getAttributeSet(i));

        }
    }

    /**
     * Computes the PLI and their entropies for all items in the next level
     * @param attributeSet
     */
    protected void preComputeNextLevelEntropies(IAttributeSet attributeSet)
    {
        System.out.printf("Computing entropy for : %s\n", attributeSet);

        int lastAttribute = attributeSet.lastAttribute();
        //iterate over all the additional attributes
        for(int i = lastAttribute + 1; i < numAttributes; i++) {
            IAttributeSet nextLevelAttributeSet = attributeSet.clone();
            nextLevelAttributeSet.add(i);
            //compute entropy, also add pli to the cache
            PositionListIndex pli = computePLI(attributeSet, i);

            //cache the entropy
            double entropy = entropyComputer(pli);
            entropyCache.put(nextLevelAttributeSet, entropy);
            entropyAccessCount.put(nextLevelAttributeSet, 0L);

            //compute entropies of all attribute sets with the same prefix
            pliCache.put(nextLevelAttributeSet, pli);
            preComputeNextLevelEntropies(nextLevelAttributeSet);
            pliCache.remove(nextLevelAttributeSet);
        }
    }

    protected PositionListIndex computePLI(IAttributeSet set, Integer newAttribute)
    {
        PositionListIndex p1 = null;
        if(set.cardinality() == 1) {
            p1 = basePLIs.get(set.nextAttribute(0));
        } else {
            p1 = pliCache.get(set);
        }
        PositionListIndex p2 = basePLIs.get(newAttribute);
        PositionListIndex p3 = p1.intersect(p2);
        return p3;
    }

    public double computeEntropy(IAttributeSet attributeSet) {
        double entropy;
        if(attributeSet.isEmpty()) {
            entropy = 0.0;
        } else if (attributeSet.cardinality() == numAttributes) {
            entropy = Math.log(numRows);
        } else {
            entropy = entropyCache.get(attributeSet);
            Long count = entropyAccessCount.get(attributeSet);
            entropyAccessCount.put(attributeSet, count + 1);
        }
        return entropy;
    }

    protected void parseLine(String line, String[] destinationArray) {
        String[] parts = line.split(",|;");
        for(int i = 0; i < numAttributes; i++) {
            destinationArray[i] = parts[i].trim();
        }
    }

    public void computeEntropies(HashMap<IAttributeSet, Double> values) {
        for(Map.Entry<IAttributeSet, Double> entry : values.entrySet()) {
            IAttributeSet attributeSet = entry.getKey();
            double entropy = computeEntropy(attributeSet);
            values.put(entry.getKey(), entropy);
        }
    }

    public void printAccessesCount() {
        System.out.println("Entropy not used!: ");
        long notUsedCount = 0;
        for(Map.Entry<IAttributeSet, Long> entry : entropyAccessCount.entrySet()) {
            if(entry.getValue() == 0) {
                System.out.printf("%s: %d\n", entry.getKey(), entry.getValue());
                notUsedCount++;
            }
        }
        System.out.printf("Count: %d\n", notUsedCount);
    }
}
