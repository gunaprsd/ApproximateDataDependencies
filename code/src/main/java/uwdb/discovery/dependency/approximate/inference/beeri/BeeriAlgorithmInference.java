package uwdb.discovery.dependency.approximate.inference.beeri;

import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.common.DataDependency;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModule;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModuleFactory;

import java.util.*;

/**
 * Implements the vanilla beeri's algorithm
 */
public class BeeriAlgorithmInference implements IInferenceModule {
    protected double alpha;
    protected RelationSchema schema;
    protected IDependencySet discoveredDependencies;

    public BeeriAlgorithmInference(IDependencySet discoveredDependencies, RelationSchema schema, double alpha) {
        this.alpha = alpha;
        this.schema = schema;
        this.discoveredDependencies = discoveredDependencies;
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

    public void infer(DataDependency dependency) {
        infer(dependency, false);
    }

    public void infer(DataDependency dependency, boolean verbose) {
        DependencyBasis dependencyBasis = new DependencyBasis(schema, dependency.lhs);
        dependencyBasis.computeAllBasis(discoveredDependencies);
        if(verbose) {
            System.out.println(dependencyBasis);
        }
        dependencyBasis.infer(dependency);
    }

    public void doBatchInference(IDependencySet inferenceSet) {
        doBatchInference(inferenceSet, false);
    }

    public void doBatchInference(IDependencySet inferenceSet, boolean verbose) {
        Iterator<IAttributeSet> iterator = inferenceSet.determinantIterator();
        while (iterator.hasNext()) {
            IAttributeSet lhs = iterator.next();
            DependencyBasis dependencyBasis = new DependencyBasis(schema, lhs);
            dependencyBasis.computeAllBasis(discoveredDependencies);
            if(verbose) {
                System.out.println(dependencyBasis);
            }

            Iterator<DataDependency> dependencyIterator = inferenceSet.iteratorOfDeterminant(lhs);
            while(dependencyIterator.hasNext()) {
                DataDependency dep = dependencyIterator.next();
                dependencyBasis.infer(dep);
            }
        }
    }

    public static class Factory implements IInferenceModuleFactory
    {
        protected RelationSchema schema;
        protected double alpha;

        public Factory(RelationSchema schema, double alpha)
        {
            this.schema = schema;
            this.alpha = alpha;
        }

        public IInferenceModule getInferenceModule(IDependencySet discoveredDependencies) {
            return new BeeriAlgorithmInference(discoveredDependencies, schema, alpha);
        }
    }
}
