package uwdb.discovery.dependency.approximate.common;


import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

import java.util.ArrayList;
import java.util.BitSet;

public class AttributeSet implements IAttributeSet
{
    protected int numAttributes;
    protected BitSet set;

    public AttributeSet(int numAttributes)
    {
        this.numAttributes = numAttributes;
        this.set = new BitSet();
    }

    public IAttributeSet clone() {
        AttributeSet other = new AttributeSet(numAttributes);
        other.set.or(this.set);
        return other;
    }

    public int length() {
        return numAttributes;
    }

    public int cardinality() {
        return set.cardinality();
    }

    public boolean isEmpty() {
        return set.isEmpty();
    }

    public void add(int index) {
        set.set(index);
    }

    public void flip(int index) {
        set.flip(index);
    }

    public void remove(int index) {
        set.clear(index);
    }

    public boolean contains(int index) {
        return set.get(index);
    }

    public void add(int fromIndex, int toIndex) {
        set.set(fromIndex, toIndex);
    }

    public void flip(int fromIndex, int toIndex) {
        set.flip(fromIndex, toIndex);
    }

    public void remove(int fromIndex, int toIndex) {
        set.clear(fromIndex, toIndex);
    }

    public IAttributeSet complement() {
        AttributeSet value = (AttributeSet)clone();
        value.flip(0, numAttributes);
        return value;
    }

    public IAttributeSet union(IAttributeSet other) {
        AttributeSet otherSet = (AttributeSet)other;
        AttributeSet value = (AttributeSet)clone();
        value.set.or(otherSet.set);
        return value;
    }

    public IAttributeSet minus(IAttributeSet other) {
        AttributeSet otherSet = (AttributeSet)other;
        AttributeSet intersectionSet = (AttributeSet) intersect(otherSet);
        AttributeSet complementSet = (AttributeSet) intersectionSet.complement();
        return intersect(complementSet);
    }

    public IAttributeSet intersect(IAttributeSet other) {
        AttributeSet otherSet = (AttributeSet)other;
        AttributeSet value = (AttributeSet)clone();
        value.set.and(otherSet.set);
        return value;
    }

    public boolean contains(IAttributeSet other) {
        for(int i = other.nextAttribute(0); i >= 0; i = other.nextAttribute(i+1)) {
            if(!contains(i)) {
                return false;
            }
        }
        return true;
    }

    public int nextAttribute(int afterIndex) {
        return set.nextSetBit(afterIndex);
    }

    public int lastAttribute() {
        return set.length() - 1;
    }

    public ArrayList<Integer> getIndices() {
        ArrayList<Integer> ids = new ArrayList<Integer>();
        for(int i = nextAttribute(0); i >= 0; i = nextAttribute(i+1)) {
            ids.add(i);
        }
        return ids;
    }

    @Override
    public int hashCode() {
        return set.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        AttributeSet other = (AttributeSet) obj;
        for (int i = nextAttribute(0), j = nextAttribute(0);
             ((i != -1) || (j != -1));
             j = other.nextAttribute(i+1), i = nextAttribute(i+1)) {
            if(i != j)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return set.toString();
    }
}
