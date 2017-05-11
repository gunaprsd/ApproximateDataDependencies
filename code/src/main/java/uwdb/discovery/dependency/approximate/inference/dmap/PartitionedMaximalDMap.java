package uwdb.discovery.dependency.approximate.inference.dmap;

import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.interfaces.IDataset;

import java.util.*;

public class PartitionedMaximalDMap {
    protected int k;
    protected IDataset dataset;
    protected RelationSchema schema;
    protected HashMap<IAttributeSet, MaximalDMap> dmaps;

    public PartitionedMaximalDMap(RelationSchema schema, IDataset dataset, int k) {
        this.schema = schema;
        this.dataset = dataset;
        this.dmaps = new HashMap<IAttributeSet, MaximalDMap>();
        this.k = k;
    }


    public void initialize() {
        HashMap<IAttributeSet, Double> entropies = new HashMap<IAttributeSet, Double>();
        for(int i = 0; i < schema.getNumAttributes(); i++) {
            entropies.put(schema.getAttributeSet(i), 0.0);
        }

        dataset.computeEntropies(entropies);

        List<Map.Entry<IAttributeSet, Double>> entries = new ArrayList<Map.Entry<IAttributeSet, Double>>(entropies.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<IAttributeSet, Double>>() {
            public int compare(Map.Entry<IAttributeSet, Double> o1, Map.Entry<IAttributeSet, Double> o2) {
                double cmp = o1.getValue() - o2.getValue();
                return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
            }
        });

        Iterator<Map.Entry<IAttributeSet, Double>> iterator = entries.iterator();
        IAttributeSet currentSet = schema.getEmptyAttributeSet();
        int currentCount = 0;
        while(iterator.hasNext()) {
            if(currentCount == k) {
                dmaps.put(currentSet, new MaximalDMap(schema, currentSet, dataset));
                currentSet = schema.getEmptyAttributeSet();
                currentCount = 0;
            }
            currentCount++;
            currentSet = currentSet.union(iterator.next().getKey());
        }

        if(!currentSet.isEmpty()) {
            dmaps.put(currentSet, new MaximalDMap(schema, currentSet, dataset));
        }

        for(Map.Entry<IAttributeSet, MaximalDMap> entry : dmaps.entrySet()) {
            MaximalDMap dmap = entry.getValue();
            dmap.initialize();
        }
    }

    public void print() {
        for(Map.Entry<IAttributeSet, MaximalDMap> entry : dmaps.entrySet()) {
            MaximalDMap dmap = entry.getValue();
            dmap.print();
        }
    }


}
