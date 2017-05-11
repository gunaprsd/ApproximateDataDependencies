package uwdb.discovery.dependency.approximate.inference.dmap;


import uwdb.discovery.dependency.approximate.common.DataDependency;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.interfaces.IDataset;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModule;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModuleFactory;

public class MaximalDMapBasedInference implements IInferenceModule {
    protected IDataset dataset;
    protected RelationSchema schema;
    protected MaximalDMap dmap;
    protected double epsilon;
    protected double alpha;

    public MaximalDMapBasedInference(RelationSchema schema, IDataset dataset, double alpha, double epsilon) {
        this.schema = schema;
        this.dataset = dataset;
        this.dmap = new MaximalDMap(schema, dataset);
        this.dmap.initialize();
        this.alpha = alpha;
        this.epsilon = epsilon;
    }

    public void infer(DataDependency dependency) {
        IAttributeSet x = dependency.lhs;
        IAttributeSet y = dependency.rhs;
        IAttributeSet z = y.complement().minus(x);
        if(!dmap.separates(x, y, z)) {
           //dependency.setMeasure(alpha + epsilon);
        }
    }

    public void infer(DataDependency dependency, boolean verbose) {
        IAttributeSet x = dependency.lhs;
        IAttributeSet y = dependency.rhs;
        IAttributeSet z = y.complement().minus(x);
        if(!dmap.separates(x, y, z)) {
            //dependency.updateMeasureLowerBound(alpha + epsilon);
        }
    }

    public void doBatchInference(IDependencySet inferenceSet) {

    }

    public void doBatchInference(IDependencySet inferenceSet, boolean verbose) {

    }

    public boolean implies(DataDependency dependency) {
        IAttributeSet x = dependency.lhs;
        IAttributeSet y = dependency.rhs;
        IAttributeSet z = y.complement().minus(x);
        if(!dmap.separates(x, y, z)) {
            return false;
        } else {
            return true;
        }
    }

    public static class Factory implements IInferenceModuleFactory
    {

        protected IDataset dataset;
        protected RelationSchema schema;
        protected double alpha;

        public Factory(IDataset dataset, RelationSchema schema, double alpha) {
            this.dataset = dataset;
            this.schema = schema;
            this.alpha = alpha;
        }

        public IInferenceModule getInferenceModule(IDependencySet discoveredDependencies) {
            return new MaximalDMapBasedInference(schema, dataset, alpha, alpha/2);
        }
    }
}
