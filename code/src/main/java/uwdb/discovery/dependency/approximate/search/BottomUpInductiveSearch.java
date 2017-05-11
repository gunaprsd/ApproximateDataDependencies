package uwdb.discovery.dependency.approximate.search;

import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.interfaces.*;
import uwdb.discovery.dependency.approximate.inference.LatticeOrderingInference;

import java.util.Iterator;
import java.util.logging.Logger;

public class BottomUpInductiveSearch implements ISearchAlgorithm {
    private static final Logger LOGGER = Logger.getLogger( TopDownInductiveSearch.class.getName() );

    protected double alpha;
    protected RelationSchema schema;
    protected DependencyType type;

    protected IInferenceModule orderInferenceModule;
    protected IInferenceModule inferenceModule;
    protected IDataset queryModule;

    protected DependencySet queue;
    protected DependencySet specializeList;
    protected DependencySet discoveredDependencies;

    public int totalInferredCount;
    public int scanCount;
    public long totalInferenceTime;
    public long totalRunningTime;
    public long totalScanningTime;

    public BottomUpInductiveSearch(DependencyType type, IDataset queryModule, IInferenceModuleFactory inferenceModuleFactory, double alpha)
    {
        this.alpha = alpha;
        this.type = type;
        this.totalInferredCount = 0;
        this.scanCount = 0;
        this.queryModule = queryModule;

        this.schema = queryModule.getSchema();
        this.discoveredDependencies = new DependencySet();
        this.inferenceModule = inferenceModuleFactory.getInferenceModule(discoveredDependencies);
        this.orderInferenceModule = new LatticeOrderingInference(schema, discoveredDependencies, alpha);
        this.queue = new DependencySet();
        this.totalInferenceTime = 0;
        this.totalRunningTime = 0;
        this.totalScanningTime = 0;
    }

    protected void initialize()
    {
        switch (type)
        {
            case FUNCTIONAL_DEPENDENCY:
                FunctionalDependency.addMostSpecificDependencies(schema, queue);
                break;
            case MULTIVALUED_DEPENDENCY:
                MultivaluedDependency.addMostSpecificDependencies(schema, queue);
                break;
        }
    }

    protected void processQueue()
    {
        long inferenceDuration = 0, scanDuration = 0;

        //infer mvdBounds from already discovered dependencies using Beeri's algorithm
        if(!discoveredDependencies.isEmpty())
        {
            long startTime = System.currentTimeMillis();
            //inferenceModule.doBatchInference(queue);
            long endTime = System.currentTimeMillis();
            inferenceDuration = (endTime - startTime);
        }

        //prepare the list to check from data
        int inferredCount = 0;
        IDependencySet checkFromData = new DependencySet();
        Iterator<DataDependency> iterator = queue.iterator();
        while (iterator.hasNext())
        {
            DataDependency dep = iterator.next();
            if(dep.isApproximate(alpha) == Status.UNKNOWN) {
                checkFromData.add(dep);
            } else {
                inferredCount++;
            }
        }

        //computeMVDBasis the value from data
        if(checkFromData.size() > 0)
        {
            scanCount++;
            long startTime = System.currentTimeMillis();
            queryModule.computeMeasures(checkFromData);
            long endTime = System.currentTimeMillis();
            scanDuration = endTime - startTime;
        }

        System.out.printf("Inferred: %d\n", inferredCount);
        System.out.printf("time taken: %dms\n", inferenceDuration);
        System.out.printf("Checked from data: %d\n", checkFromData.size());
        System.out.printf("time taken: %dms\n", scanDuration);

        totalInferenceTime += inferenceDuration;
        totalScanningTime += scanDuration;
        totalInferredCount += inferredCount;
    }

    protected void createQueue()
    {
        queue = new DependencySet();
        Iterator<DataDependency> iterator = specializeList.iterator();
        while (iterator.hasNext()) {
            DataDependency parent = iterator.next();
            parent.addSpecializations(orderInferenceModule, queue);
        }
    }

    public DependencySet getDiscoveredDataDependencies()
    {
        return discoveredDependencies;
    }

    public void search()
    {
        System.out.println("Search Progress:");
        long startTime = System.currentTimeMillis();
        initialize();
        int level = 0;
        while(!queue.isEmpty())
        {
            level++;
            System.out.printf("Level: %d\n", level);
            System.out.printf("Queue size: %d\n", queue.size());

            processQueue();
            specializeList = new DependencySet();

            Iterator<DataDependency> iterator = queue.iterator();
            while (iterator.hasNext())
            {
                DataDependency dependency = iterator.next();
                switch (dependency.isApproximate(alpha))
                {
                    case TRUE:
                        discoveredDependencies.add(dependency);
                        break;
                    case FALSE:
                        specializeList.add(dependency);
                        break;
                    case UNKNOWN:
                        throw new RuntimeException("Must be decided by now!");
                }
            }

            System.out.printf("Failed: %d\n", specializeList.size());
            System.out.println();

            createQueue();
        }

        long endTime = System.currentTimeMillis();
        totalRunningTime = endTime - startTime;
    }

    public void printDiscoveredDependencies()
    {
        System.out.println("Discovered Data Dependencies:");
        Iterator<DataDependency> iterator = discoveredDependencies.iterator();
        while(iterator.hasNext())
        {
            System.out.println(iterator.next());
        }
        System.out.println();
    }

    public void printRuntimeCharacteristics()
    {
        System.out.println("Runtime Characteristics:");
        System.out.printf("Dependency Type: %s\n", type.toString());
        System.out.printf("Num Attributes: %d\n", queryModule.getNumAttributes());
        System.out.printf("Num Rows: %d\n", queryModule.getNumRows());
        System.out.printf("Threshold: %f\n", alpha);
        System.out.printf("Total Discovered: %d\n", discoveredDependencies.size());
        System.out.printf("Num Inferred: %d\n", totalInferredCount);
        System.out.printf("Num File Scans: %d\n", scanCount);
        System.out.printf("Running Time: %dms\n", totalRunningTime);
        System.out.printf("File Scan Time: %dms\n", totalScanningTime);
        System.out.printf("Inference Time: %dms\n", totalInferenceTime);
    }
}
