package uwdb.discovery.dependency.approximate.interfaces;

import uwdb.discovery.dependency.approximate.common.DataDependency;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;

public interface IInferenceModule {
    void infer(DataDependency dependency);
    void infer(DataDependency dependency, boolean verbose);
    void doBatchInference(IDependencySet inferenceSet);
    void doBatchInference(IDependencySet inferenceSet, boolean verbose);
    boolean implies(DataDependency dependency);
}
