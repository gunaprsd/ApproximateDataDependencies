package uwdb.discovery.dependency.approximate.entropy;


import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.DataDependency;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDataset;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;

import java.util.*;

public abstract class AbstractDataSet implements IDataset {

    protected long numRows;
    protected int numAttributes;
    protected RelationSchema schema;

    public AbstractDataSet(RelationSchema schema)
    {
        this.schema = schema;
        this.numAttributes = schema.getNumAttributes();
    }

    public long getNumRows()
    {
        return numRows;
    }

    public int getNumAttributes() {
        return schema.getNumAttributes();
    }

    public void computeMeasures(IDependencySet dependencies) {
        HashMap<IAttributeSet, Double> entropies = new HashMap<IAttributeSet, Double>();
        addEntropiesForComputingMeasures(dependencies, entropies);
        computeEntropies(entropies);
        computerMeasuresFromEntropies(dependencies, entropies);
    }

    public void computeMeasures(IDependencySet dependencies, IAttributeSet subset) {
        HashMap<IAttributeSet, Double> entropies = new HashMap<IAttributeSet, Double>();
        addEntropiesForComputingMeasures(dependencies, entropies, subset);
        computeEntropies(entropies);
        computerMeasuresFromEntropies(dependencies, entropies, subset);
    }

    protected void addEntropiesForComputingMeasures(IDependencySet dependencies, HashMap<IAttributeSet, Double> subsets)
    {
        Iterator<DataDependency> iterator = dependencies.iterator();
        while (iterator.hasNext())
        {
            DataDependency dependency = iterator.next();
            switch (dependency.getType())
            {
                case FUNCTIONAL_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Add X, XY
                    subsets.put(X, Double.MAX_VALUE);
                    subsets.put(XY, Double.MAX_VALUE);
                }
                break;
                case MULTIVALUED_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Create X(R-XY) = R-Y
                    IAttributeSet R_Y = dependency.rhs.complement();

                    //Add X, XY, R-Y
                    subsets.put(X, Double.MAX_VALUE);
                    subsets.put(XY, Double.MAX_VALUE);
                    subsets.put(R_Y, Double.MAX_VALUE);
                }
                break;
                default:
                    break;
            }
        }
    }


    protected void addEntropiesForComputingMeasures(IDependencySet dependencies, HashMap<IAttributeSet, Double> subsets, IAttributeSet subset)
    {
        Iterator<DataDependency> iterator = dependencies.iterator();
        while (iterator.hasNext())
        {
            DataDependency dependency = iterator.next();
            switch (dependency.getType())
            {
                case FUNCTIONAL_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Add X, XY
                    subsets.put(X, Double.MAX_VALUE);
                    subsets.put(XY, Double.MAX_VALUE);
                }
                break;
                case MULTIVALUED_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Create X(R-XY) = R-Y
                    IAttributeSet R_Y = dependency.rhs.complement().intersect(subset);

                    //Add X, XY, R-Y
                    subsets.put(X, Double.MAX_VALUE);
                    subsets.put(XY, Double.MAX_VALUE);
                    subsets.put(R_Y, Double.MAX_VALUE);
                    subsets.put(subset, Double.MAX_VALUE);
                }
                break;
                default:
                    break;
            }
        }
    }

    protected void computerMeasuresFromEntropies(IDependencySet dependencies, HashMap<IAttributeSet, Double> entropies)
    {
        Iterator<DataDependency> iterator = dependencies.iterator();
        while (iterator.hasNext())
        {
            DataDependency dependency = iterator.next();
            switch (dependency.getType())
            {
                case FUNCTIONAL_DEPENDENCY:
                {
                    int hashCode1, hashCode2;
                    double entropy1, entropy2;

                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    double entropy_X = entropies.get(X);
                    double entropy_XY = entropies.get(XY);
                    double measure = entropy_XY - entropy_X;

                    try {
                        dependency.setMeasure(measure);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
                case MULTIVALUED_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Create X(R-XY) = R-Y
                    IAttributeSet R_Y = dependency.rhs.complement();

                    double entropy_X = entropies.get(X);
                    double entropy_XY = entropies.get(XY);
                    double entropy_R_Y = entropies.get(R_Y);
                    double entropy_R = getTotalEntropy();
                    double measure = entropy_XY + entropy_R_Y - entropy_R - entropy_X;

                    try {
                        dependency.setMeasure(measure);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
                default:
                    break;
            }
        }
    }

    protected void computerMeasuresFromEntropies(IDependencySet dependencies, HashMap<IAttributeSet, Double> entropies, IAttributeSet subset)
    {
        Iterator<DataDependency> iterator = dependencies.iterator();
        while (iterator.hasNext())
        {
            DataDependency dependency = iterator.next();
            switch (dependency.getType())
            {
                case FUNCTIONAL_DEPENDENCY:
                {
                    int hashCode1, hashCode2;
                    double entropy1, entropy2;

                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    double entropy_X = entropies.get(X);
                    double entropy_XY = entropies.get(XY);
                    double measure = entropy_XY - entropy_X;

                    try {
                        dependency.setMeasure(measure);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
                case MULTIVALUED_DEPENDENCY:
                {
                    //Create X
                    IAttributeSet X = dependency.lhs;

                    //Create XY
                    IAttributeSet XY = dependency.lhs.union(dependency.rhs);

                    //Create X(R-XY) = R-Y
                    IAttributeSet R_Y = dependency.rhs.complement().intersect(subset);

                    double entropy_X = entropies.get(X);
                    double entropy_XY = entropies.get(XY);
                    double entropy_R_Y = entropies.get(R_Y);
                    double entropy_R = entropies.get(subset);
                    double measure = entropy_XY + entropy_R_Y - entropy_R - entropy_X;

                    try {
                        dependency.setMeasure(measure);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
                default:
                    break;
            }
        }
    }
    protected void addCount(HashMap<Integer, Long> counts, long[] key, IAttributeSet set)
    {
        int hashCode = Utilities.getHashCode(key, set);
        if(!counts.containsKey(hashCode))
        {
            counts.put(hashCode, (long)1);
        }
        else
        {
            Long value = counts.get(hashCode);
            value += 1;
            counts.put(hashCode, value);
        }
    }

    protected void addCount(HashMap<Integer, Long> counts, String[] key, IAttributeSet set)
    {
        int hashCode = Utilities.getHashCode(key, set);
        if(!counts.containsKey(hashCode))
        {
            counts.put(hashCode, (long)1);
        }
        else
        {
            Long value = counts.get(hashCode);
            value += 1;
            counts.put(hashCode, value);
        }
    }

    protected double computeEntropyValue(HashMap<Integer, Long> counts, long totalCount)
    {
        double entropy = 0;
        for(Map.Entry<Integer,Long> entry : counts.entrySet())
        {
            double prob = (double)entry.getValue() / (double)totalCount;
            if(prob > 0)
            {
                entropy += prob * Math.log(prob);
            }
        }

        return -entropy;
    }

    protected double getTotalEntropy()
    {
        return Math.log(numRows);
    }

}
