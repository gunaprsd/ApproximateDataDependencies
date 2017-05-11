package uwdb.discovery.dependency.approximate.inference.beeri;

import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class DependencyBasis {

    protected IAttributeSet X;
    protected RelationSchema schema;
    protected HashSet<Integer> fdBasis;
    protected HashSet<IAttributeSet> mvdBasis;
    protected HashMap<Integer, Double> fdBounds;
    protected HashMap<IAttributeSet, Double> mvdBounds;


    public DependencyBasis(RelationSchema schema, IAttributeSet X) {
        this.X = X;
        this.schema = schema;

        this.fdBasis = new HashSet<Integer>();
        this.mvdBasis = new HashSet<IAttributeSet>();

        this.fdBounds = new HashMap<Integer, Double>();
        this.mvdBounds = new HashMap<IAttributeSet, Double>();
    }


    public void computeMVDBasis(IDependencySet discoveredDependencies) {
        HashSet<IAttributeSet> queue = new HashSet<IAttributeSet>();

        //Adding all attributes in X to the queue
        for (int i = X.nextAttribute(0); i >= 0; i = X.nextAttribute(i+1)) {
            IAttributeSet A = schema.getEmptyAttributeSet();
            A.add(i);
            queue.add(A);
            mvdBounds.put(A, 0.0);
        }

        //Adding R-X to the queue
        IAttributeSet complement = X.complement();
        queue.add(complement);
        mvdBounds.put(complement, 0.0);

        //addSet and removeSet required to prevent modification
        // of data structure when an iterator is active
        HashSet<IAttributeSet> addSet = new HashSet<IAttributeSet>();
        HashSet<IAttributeSet> removeSet = new HashSet<IAttributeSet>();

        boolean converged = false, split = false;
        while(!converged)
        {
            split = false;
            converged = true;

            Iterator<IAttributeSet> pieceIterator = queue.iterator();
            while (pieceIterator.hasNext()) {
                //For X ->-> b, we need to split b with respect to those S ->-> T such that
                // (S \cap b) is empty, (T \cap b) and (T^c \cap b) is non-empty
                IAttributeSet b = pieceIterator.next();

                Iterator<DataDependency> dependencyIterator = discoveredDependencies.iterator();
                while (dependencyIterator.hasNext()) {
                    DataDependency dep = dependencyIterator.next();

                    IAttributeSet S = dep.lhs;
                    IAttributeSet T = dep.rhs;

                    IAttributeSet S_int_b = S.intersect(b);
                    IAttributeSet T_int_b = T.intersect(b);
                    IAttributeSet Tc_int_b = T.complement().intersect(b);

                    if(S_int_b.isEmpty() && !T_int_b.isEmpty() && !Tc_int_b.isEmpty()) {
                        split = true;

                        double value = dep.getMeasureUpperBound();
                        value += mvdBounds.get(b);

                        removeSet.add(b);

                        addSet.add(T_int_b);
                        addBound(T_int_b, value);

                        addSet.add(Tc_int_b);
                        addBound(Tc_int_b, value);
                    }
                }

                if(split) {
                    converged = false;
                } else {
                    mvdBasis.add(b);
                    removeSet.add(b);
                }
            }

            //if no splitting happened in the complete
            //loop then it has converged!
            if(!converged) {
                queue.removeAll(removeSet);
                queue.addAll(addSet);
                removeSet.clear();
                addSet.clear();
            }
        }
    }

    protected void addBound(IAttributeSet set, double value) {
        if(mvdBounds.containsKey(set))
        {
            double previousValue = mvdBounds.get(set);
            mvdBounds.put(set, Math.min(previousValue, value));
        }
        else
        {
            mvdBounds.put(set, value);
        }
    }

    public void infer(DataDependency dependency) {
        switch (dependency.getType()) {
            case MULTIVALUED_DEPENDENCY:
                {
                    double value = 0.0;
                    IAttributeSet currentUnion = schema.getEmptyAttributeSet();
                    for(IAttributeSet b : mvdBasis) {
                        if(dependency.rhs.contains(b)) {
                            currentUnion = currentUnion.union(b);
                            value += mvdBounds.get(b);
                        }
                    }

                    if(currentUnion.contains(dependency.rhs) && dependency.rhs.contains(currentUnion))
                    {
                        dependency.updateMeasureUpperBound(value);
                    }
                }
                break;
            case FUNCTIONAL_DEPENDENCY:
                {
                    double value = 0.0;
                    for(int i = dependency.rhs.nextAttribute(0); i >= 0; i = dependency.rhs.nextAttribute(i+1)) {
                        if(!fdBasis.contains(i)) {
                            return;
                        } else {
                            value += fdBounds.get(i);
                        }
                    }
                    dependency.updateMeasureUpperBound(value);
                }
                break;
            default:
                break;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("\nDependency Bases:\n");
        builder.append("DEP(");
        builder.append(X);
        builder.append(") = ");
        builder.append("{");
        boolean start = true;
        for (IAttributeSet attributeSet : mvdBasis) {
            if(!start) {
                builder.append(", ");
            } else {
                start = false;
            }
            builder.append(attributeSet);
        }

        builder.append("}\n");

        start = true;
        builder.append(X);
        builder.append("* = ");
        builder.append("{");
        for (Integer attribute : fdBasis) {
            if(!start) {
                builder.append(", ");
            } else {
                start = false;
            }
            builder.append(attribute);
        }
        builder.append("}\n");

        return builder.toString();
    }

    public void computeFDBasis(IDependencySet discoveredDependencies)
    {
        //Add all the trivial fds
        for(int i = X.nextAttribute(0); i >= 0; i = X.nextAttribute(i+1)) {
            fdBasis.add(i);
            fdBounds.put(i, 0.0);
        }

        Iterator<IAttributeSet> iterator = mvdBasis.iterator();
        while(iterator.hasNext()) {
            IAttributeSet attributeSet = iterator.next();

            //If the mvdBasis has a singleton attribute
            if(attributeSet.cardinality() == 1) {
                Integer attributeId = attributeSet.nextAttribute(0);

                //don't waste time if already in basis
                if(!fdBasis.contains(attributeId)) {

                    //check if there is an FD in discoveredDependencies with this attribute
                    Iterator<DataDependency> dependencyIterator = discoveredDependencies.iteratorOfDeterminate(attributeSet);
                    if(dependencyIterator != null) {
                        while (dependencyIterator.hasNext()) {
                            DataDependency dep = dependencyIterator.next();

                            if(dep.getType() == DependencyType.FUNCTIONAL_DEPENDENCY) {

                                double value = mvdBounds.get(attributeSet);
                                value += dep.getMeasureUpperBound();

                                fdBasis.add(attributeId);
                                fdBounds.put(attributeId, value);
                            }
                        }
                    }
                }
            }
        }
    }

    public void computeAllBasis(IDependencySet discoveredDependencies)
    {
        //order is important!
        computeMVDBasis(discoveredDependencies);
        computeFDBasis(discoveredDependencies);
    }
}
