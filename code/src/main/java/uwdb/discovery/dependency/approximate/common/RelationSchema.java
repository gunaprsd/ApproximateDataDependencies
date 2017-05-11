package uwdb.discovery.dependency.approximate.common;


import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

public class RelationSchema {
    protected int numAttributes;

    public RelationSchema(int numAttributes) {
        this.numAttributes = numAttributes;
    }

    public int getNumAttributes()
    {
        return numAttributes;
    }

    public IAttributeSet getEmptyAttributeSet()
    {
        return new AttributeSet(numAttributes);
    }

    public IAttributeSet getAttributeSet(int... indices)
    {
        IAttributeSet attributeSet =  new AttributeSet(numAttributes);
        for (int index : indices) {
            attributeSet.add(index);
        }
        return attributeSet;
    }
}
