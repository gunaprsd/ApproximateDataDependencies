package uwdb.discovery.dependency.approximate.inference;

import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.DataDependency;
import uwdb.discovery.dependency.approximate.common.DependencySet;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModule;

import java.util.Iterator;

public class LatticeOrderingInference implements IInferenceModule {
    protected double alpha;
    protected RelationSchema schema;
    protected DependencySet discoveredDependencies;

    public LatticeOrderingInference(RelationSchema schema, DependencySet discoveredDependencies, double alpha) {
        this.alpha = alpha;
        this.schema = schema;
        this.discoveredDependencies = discoveredDependencies;
    }

    public void infer(DataDependency dependency) {
        infer(dependency, false);
    }

    public void infer(DataDependency dependency, boolean verbose) {
        Iterator<DataDependency> dependencyIterator = discoveredDependencies.iteratorOfDeterminate(dependency.rhs);
        if(dependencyIterator != null) {
            while (dependencyIterator.hasNext()) {
                DataDependency discoveredDependency = dependencyIterator.next();
                if(dependency.lhs.contains(discoveredDependency.lhs)) {
                    double value = discoveredDependency.getMeasureUpperBound();
                    dependency.updateMeasureUpperBound(value);
                    break;
                }
            }
        }
    }

    public void doBatchInference(IDependencySet inferenceSet) {
        //
    }

    public void doBatchInference(IDependencySet inferenceSet, boolean verbose) {

    }

    public boolean implies(DataDependency dependency) {
        infer(dependency);
        switch (dependency.isApproximate(alpha)) {
            case TRUE:
                return true;
            default:
                return false;
        }
    }
}
