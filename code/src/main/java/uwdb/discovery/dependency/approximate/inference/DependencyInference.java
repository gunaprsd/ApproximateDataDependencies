package uwdb.discovery.dependency.approximate.inference;


import uwdb.discovery.dependency.approximate.common.DataDependency;
import uwdb.discovery.dependency.approximate.common.DependencySet;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;
import uwdb.discovery.dependency.approximate.common.RelationSchema;
import uwdb.discovery.dependency.approximate.inference.beeri.BeeriAlgorithmInference;
import uwdb.discovery.dependency.approximate.interfaces.IInferenceModule;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;

public class DependencyInference {
    public static String fileName;
    public static RelationSchema schema;
    public static IDependencySet givenDependencies;
    public static IDependencySet dependenciesToInfer;

    public static void initialize()
    {
        givenDependencies = new DependencySet();
        dependenciesToInfer = new DependencySet();
        loadFile();
    }

    /**
     * File must be of the following structure:
     * 1. number of attributes in the first line
     * 2. empty line
     * 2. followed by a list of approximate or exact data dependencies
     * 3. empty line
     * 4. list of dependencies to be inferred
     */
    public static void loadFile()
    {

        String line = null;
        BufferedReader reader = null;
        int phase = 0;
        int numAttributes = 0;
        DataDependency dependency = null;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            while((line  = reader.readLine()) != null) {
                if(line.trim().equals("")) {
                    phase++;
                    continue;
                } else {
                    switch (phase)
                    {
                        case 0:
                            numAttributes = Integer.parseInt(line.trim());
                            schema = new RelationSchema(numAttributes);
                            break;
                        case 1:
                            dependency = DataDependency.parseDependencyString(schema, line.trim(), true);
                            givenDependencies.add(dependency);
                            break;
                        case 2:
                            dependency = DataDependency.parseDependencyString(schema, line.trim(), false);
                            dependenciesToInfer.add(dependency);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void runInference() {
        IInferenceModule module = new BeeriAlgorithmInference(givenDependencies, schema, 0.0);
        module.doBatchInference(dependenciesToInfer, true);
    }

    public static void main(String[] args) {
        fileName = args[0];

        initialize();

        System.out.println("Given:");
        Iterator<DataDependency> iterator = givenDependencies.iterator();
        while(iterator.hasNext()) {
            DataDependency dependency = iterator.next();
            System.out.println(dependency);
        }

        runInference();

        System.out.println("Inferred Bounds:");
        iterator = dependenciesToInfer.iterator();
        while(iterator.hasNext()) {
            DataDependency dependency = iterator.next();
            System.out.println(dependency);
        }
    }
}
