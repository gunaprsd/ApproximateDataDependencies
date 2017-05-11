package uwdb.discovery.dependency.approximate.interfaces;


public interface IInferenceModuleFactory {
    IInferenceModule getInferenceModule(IDependencySet discoveredDependencies);
}
