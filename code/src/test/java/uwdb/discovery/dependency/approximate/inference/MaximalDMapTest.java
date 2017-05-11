package uwdb.discovery.dependency.approximate.inference;

import org.junit.Test;
import uwdb.discovery.dependency.approximate.Utilities;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.entropy.ExternalFileDataSet;
import uwdb.discovery.dependency.approximate.inference.dmap.MaximalDMap;


public class MaximalDMapTest {
    @Test
    public void getCDEP() throws Exception {
        Utilities.initialize();
        String choice = "horse";
        RelationSchema schema = new RelationSchema(Utilities.numCols.get(choice));
        ExternalFileDataSet dataSet = new ExternalFileDataSet(Utilities.filePath.get(choice), schema, true);
        dataSet.initialize();
//        IAttributeSet subset = schema.getAttributeSet(0, 1, 2, 4, 5, 22, 25, 13, 19, 3, 21);
        MaximalDMap maximalDMap = new MaximalDMap(schema, dataSet);
        maximalDMap.initialize();
        maximalDMap.print();
//        IAttributeSet attrs = schema.getAttributeSet(3, 6, 7);
//        ArrayList<IAttributeSet> cdep = maximalDMap.getCDEP(attrs);
//        for(int i = 0; i < schema.getNumAttributes(); i++) {
//            System.out.printf("%d : %s\n", i, maximalDMap.getPotentialMinimalFD(i));
//        }

//        PartitionedMaximalDMap pDmap = new PartitionedMaximalDMap(schema, dataSet, 6);
//        pDmap.initialize();
//        pDmap.print();
    }
}