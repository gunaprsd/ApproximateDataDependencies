package uwdb.discovery.dependency.approximate.interfaces;

import uwdb.discovery.dependency.approximate.common.DependencySet;
import uwdb.discovery.dependency.approximate.common.DependencyType;
import uwdb.discovery.dependency.approximate.common.Status;

public interface IDataDependency {
    Status isExact();
    Status isApproximate(double alpha);
    boolean addSpecializations(DependencySet destination);
    boolean addSpecializations(IInferenceModule inferenceModule, DependencySet destination);
    boolean addSpecializations(IInferenceModule inferenceModule, IInferenceModule pruningModule, DependencySet destination);
    boolean addGeneralizations(DependencySet destination);
    DependencyType getType();
}
