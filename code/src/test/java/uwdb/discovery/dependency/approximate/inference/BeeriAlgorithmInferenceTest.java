package uwdb.discovery.dependency.approximate.inference;

import org.junit.Test;
import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.inference.beeri.BeeriAlgorithmInference;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModule;

import static org.junit.Assert.*;

public class BeeriAlgorithmInferenceTest {
    protected static RelationSchema schema;
    protected static IAttributeSet U, W, X, Y, Z;

    public static void initialize()
    {
        schema = new RelationSchema(5);
        U = schema.getAttributeSet(0);
        W = schema.getAttributeSet(1);
        X = schema.getAttributeSet(2);
        Y = schema.getAttributeSet(3);
        Z = schema.getAttributeSet(4);
    }

    @Test
    public void augmentation()
    {
        initialize();
        IAttributeSet YW = Y.union(W);
        IAttributeSet XW = X.union(W);

        DependencySet dependencySet = new DependencySet();
        MultivaluedDependency d1 = new MultivaluedDependency(X, Y, 0.0, 0.0);
        MultivaluedDependency d2 = new MultivaluedDependency(YW, Z, 0.0, 0.0);
        MultivaluedDependency d3 = new MultivaluedDependency(XW, Y);
        dependencySet.add(d1);
        dependencySet.add(d2);

        IInferenceModule beeriAlgorithm = new BeeriAlgorithmInference(dependencySet, schema, 0.0);
        assertTrue(beeriAlgorithm.implies(d3));
    }

    @Test
    public void transitivity()
    {
        initialize();
        IAttributeSet ZW = Z.union(W);
        IAttributeSet ZWY = ZW.union(Y);

        DependencySet dependencySet = new DependencySet();
        MultivaluedDependency d1 = new MultivaluedDependency(X, Y, 0.0, 0.0);
        MultivaluedDependency d2 = new MultivaluedDependency(Y, ZWY, 0.0, 0.0);
        MultivaluedDependency d3 = new MultivaluedDependency(X, ZW);
        dependencySet.add(d1);
        dependencySet.add(d2);

        IInferenceModule beeriAlgorithm = new BeeriAlgorithmInference(dependencySet, schema, 0.0);
        assertTrue(beeriAlgorithm.implies(d3));

        dependencySet = new DependencySet();
        d1 = new MultivaluedDependency(X, Y, 0.0, 0.0);
        d2 = new MultivaluedDependency(Y, Z, 0.0, 0.0);
        d3 = new MultivaluedDependency(X, Z);
        dependencySet.add(d1);
        dependencySet.add(d2);

        beeriAlgorithm = new BeeriAlgorithmInference(dependencySet, schema, 0.0);
        assertTrue(beeriAlgorithm.implies(d3));
    }

    @Test
    public void pseudoTransitivity()
    {
        initialize();
        IAttributeSet XW = X.union(W);
        IAttributeSet YW = Y.union(W);
        IAttributeSet ZW = Z.union(W);
        IAttributeSet ZWY = ZW.union(Y);
        IAttributeSet ZU = Z.union(U);
        IAttributeSet ZUY = ZU.union(Y);

        DependencySet dependencySet = new DependencySet();
        MultivaluedDependency d1 = new MultivaluedDependency(X, Y, 0.0, 0.0);
        MultivaluedDependency d2 = new MultivaluedDependency(YW, ZUY, 0.0, 0.0);
        MultivaluedDependency d3 = new MultivaluedDependency(XW, ZU);
        dependencySet.add(d1);
        dependencySet.add(d2);

        BeeriAlgorithmInference beeriAlgorithm = new BeeriAlgorithmInference(dependencySet, schema, 0.0);
        assertTrue(beeriAlgorithm.implies(d3));
    }

    @Test
    public void union()
    {
        initialize();

        IAttributeSet YZ = Y.union(Z);

        DependencySet dependencySet = new DependencySet();
        MultivaluedDependency d1 = new MultivaluedDependency(X, Y, 0.0, 0.0);
        MultivaluedDependency d2 = new MultivaluedDependency(X, Z, 0.0, 0.0);
        MultivaluedDependency d3 = new MultivaluedDependency(X, YZ);

        dependencySet.add(d1);
        dependencySet.add(d2);

        BeeriAlgorithmInference algorithm = new BeeriAlgorithmInference(dependencySet, schema, 0.0);
        assertTrue(algorithm.implies(d3));
    }

}