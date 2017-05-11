package uwdb.discovery.dependency.approximate.interfaces;

import uwdb.discovery.dependency.approximate.common.DependencySet;

public interface ISearchAlgorithm {
    DependencySet getDiscoveredDataDependencies();
    void search();
}
