package uwdb.discovery.dependency.approximate.interfaces;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;

import java.util.HashMap;

public interface IDataset {
    RelationSchema getSchema();
    int getNumAttributes();
    long getNumRows();
    void initialize();
    double computeEntropy(IAttributeSet subset);
    void computeEntropies(HashMap<IAttributeSet, Double> entropies);
    void computeMeasures(IDependencySet dependencies);
    void computeMeasures(IDependencySet dependencies, IAttributeSet subset);
}
