package uwdb.discovery.dependency.approximate.common;


import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

public class RelationSchema {
    protected String[] header;
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

    public void setHeader(String headerString) {
        this.header = headerString.split(",");
    }

    public String getAttributeSetString(IAttributeSet set) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean start = true;
        for(int i = set.nextAttribute(0); i >= 0; i = set.nextAttribute(i+1)) {
            if(!start) {
                builder.append(", ");
            }
            builder.append(header[i]);
            start = false;
        }
        builder.append("}");
        return builder.toString();
    }

    public String getAttributeString(int index) {
        return header[index];
    }
}
