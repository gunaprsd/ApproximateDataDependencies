package uwdb.discovery.dependency.approximate.interfaces;

import java.util.ArrayList;

public interface IAttributeSet {
    int length();
    int cardinality();
    boolean isEmpty();

    void add(int index);
    void flip(int index);
    void remove(int index);
    boolean contains(int index);

    void add(int fromIndex, int toIndex);
    void flip(int fromIndex, int toIndex);
    void remove(int fromIndex, int toIndex);

    IAttributeSet clone();
    IAttributeSet complement();
    IAttributeSet union(IAttributeSet other);
    IAttributeSet minus(IAttributeSet other);
    IAttributeSet intersect(IAttributeSet other);

    boolean contains(IAttributeSet other);

    //afterIndex or beforeIndex are inclusive
    int nextAttribute(int afterIndex);
    int lastAttribute();
    ArrayList<Integer> getIndices();
}
