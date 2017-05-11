package uwdb.discovery.dependency.approximate.common;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDataDependency;

public abstract class DataDependency implements IDataDependency {
    public IAttributeSet lhs;
    public IAttributeSet rhs;
    public Measure measure;

    public DataDependency(IAttributeSet lhs, IAttributeSet rhs, double lowerBound, double upperBound)
    {
        this.lhs = lhs;
        this.rhs = rhs;
        this.measure = new Measure(lowerBound, upperBound);
    }

    public Status isExact() {
        return isApproximate(0.0);
    }
    
    public Status isApproximate(double alpha) {
        return measure.isLessThan(alpha);
    }

    public void setMeasure(double value)
    {
        try {
            measure.setValue(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public double getMeasureUpperBound()
    {
        return measure.upperBound;
    }

    public double getMeasureLowerBound()
    {
        return measure.lowerBound;
    }

    public void updateMeasureUpperBound(double value)
    {
        measure.updateUpperBound(value);
    }

    public void updateMeasureLowerBound(double value)
    {
        measure.updateLowerBound(value);
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(lhs.hashCode());
        builder.append(rhs.hashCode());
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        FunctionalDependency other = (FunctionalDependency) obj;
        return lhs.equals(other.lhs) && rhs.equals(other.rhs);
    }


    /**
     * Parses lines of the form {0, 1, 2, 3} -> {4, 6, 5} [1.456, 2343.234]
     * @param dependencyString
     * @return
     */
    public static DataDependency parseDependencyString(RelationSchema schema, String dependencyString, boolean exact)
    {
        String[] parts = dependencyString.split("\\{|\\}|\\[|\\]");
        //obtain lhs items
        String[] determinantAttributes = parts[1].split(",");
        String[] determinateAttributes = parts[3].split(",");
        String type = parts[2];
        String[] bounds = null;
        if(parts.length >= 6)
        {
            bounds = parts[5].split(",");
        }

        DataDependency dependency = null;
        IAttributeSet lhs = schema.getEmptyAttributeSet();
        IAttributeSet rhs = schema.getEmptyAttributeSet();
        double lowerBound = 0, upperBound = Double.MAX_VALUE;
        if(exact) {
            upperBound = 0;
        }

        try {
            for(String attr : determinantAttributes) {
                int index = Integer.parseInt(attr.trim());
                lhs.add(index);
            }

            for(String attr : determinateAttributes) {
                int index = Integer.parseInt(attr.trim());
                rhs.add(index);
            }

            if(parts.length >= 6) {
                lowerBound = Double.parseDouble(bounds[0].trim());
                upperBound = Double.parseDouble(bounds[1].trim());
            }

            if(type.trim().equals("->")) {
                dependency = new FunctionalDependency(lhs, rhs, lowerBound, upperBound);
            } else if(type.trim().equals("->>")) {
                dependency = new MultivaluedDependency(lhs, rhs, lowerBound, upperBound);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return dependency;
    }

}
