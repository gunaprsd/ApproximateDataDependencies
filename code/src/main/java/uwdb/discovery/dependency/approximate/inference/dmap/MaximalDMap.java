package uwdb.discovery.dependency.approximate.inference.dmap;


import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.interfaces.IDataset;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDependencySet;

import java.util.*;

public class MaximalDMap
{
    protected RelationSchema schema;
    protected IAttributeSet subset;
    protected HashMap<Integer, DMapNode> vertices;
    protected IDataset dataset;

    public MaximalDMap(RelationSchema schema, IAttributeSet subsetAttributes, IDataset dataset) {
        this.schema = schema;
        this.dataset = dataset;
        this.vertices = new HashMap<Integer, DMapNode>();
        this.subset = subsetAttributes;
    }

    public MaximalDMap(RelationSchema schema, IDataset dataset) {
        this.schema = schema;
        this.dataset = dataset;
        this.vertices = new HashMap<Integer, DMapNode>();
        this.subset = schema.getEmptyAttributeSet();
        for(int i = 0; i < schema.getNumAttributes(); i++) {
            this.subset.add(i);
        }
    }

    public void initialize()
    {
        int numAttributes = schema.getNumAttributes();
        for(int i = subset.nextAttribute(0); i >= 0; i = subset.nextAttribute(i+1)) {
            vertices.put(i, new DMapNode(i));
        }

        IDependencySet deps = new DependencySet();
        for(int i = subset.nextAttribute(0); i >= 0; i = subset.nextAttribute(i+1)) {
            for(int j = subset.nextAttribute(0); j >= 0; j = subset.nextAttribute(j+1)) {
                if(i < j) {
                    IAttributeSet lhs = schema.getAttributeSet(i, j).complement().intersect(subset);
                    IAttributeSet rhs = schema.getAttributeSet(i);
                    MultivaluedDependency dep = new MultivaluedDependency(lhs, rhs);
                    deps.add(dep);
                }
            }
        }

        dataset.computeMeasures(deps, subset);

        IAttributeSet intersection = schema.getEmptyAttributeSet().complement().intersect(subset);
        Iterator<DataDependency> iterator = deps.iterator();
        while (iterator.hasNext()) {
            MultivaluedDependency mvd = (MultivaluedDependency)iterator.next();
            if(mvd.isApproximate(1E-6) == Status.FALSE) {
                IAttributeSet lhs_c = mvd.lhs.complement().intersect(subset);
                int i = lhs_c.nextAttribute(0);
                int j = lhs_c.nextAttribute(i+1);
                vertices.get(i).connect(vertices.get(j));
            } else {
                //System.out.println(mvd.lhs);
                intersection = intersection.intersect(mvd.lhs);
            }
        }

    }

    public void print()
    {
        System.out.printf("Maximal D-Map for %s:\n", subset);
        for(DMapNode n : vertices.values()) {
            System.out.printf("Node %d : ", n.id);
            for(DMapNode nbr : n.nbrs) {
                System.out.printf("%d, ", nbr.id);
            }
            System.out.println();
        }
    }

    public ArrayList<IAttributeSet> getCDEP(IAttributeSet attributes)
    {
        ArrayList<Integer> notVisited = new ArrayList<Integer>();
        for(int i = 0; i < vertices.size(); i++) {
            if(!attributes.contains(i)) {
                notVisited.add(i);
            }
        }

        ArrayList<IAttributeSet> basis = new ArrayList<IAttributeSet>();
        while (!notVisited.isEmpty()) {
            IAttributeSet b = schema.getEmptyAttributeSet();
            LinkedList<Integer> queue = new LinkedList<Integer>();

            Integer startId = notVisited.get(0);
            queue.add(startId);
            notVisited.remove(startId);

            while (!queue.isEmpty()) {
                Integer nodeId = queue.poll();
                b.add(nodeId);

                DMapNode node = vertices.get(nodeId);
                for(int i = 0; i < node.nbrs.size(); i++) {
                    Integer nbrId = node.nbrs.get(i).id;
                    if(!attributes.contains(nbrId) && notVisited.contains(nbrId))
                    {
                        notVisited.remove(nbrId);
                        queue.add(nbrId);
                    }
                }
            }

            basis.add(b);
        }
        return basis;
    }

    public IAttributeSet getPotentialMinimalFD(Integer attribute)
    {
        DMapNode node = vertices.get(attribute);
        IAttributeSet nbrs = schema.getEmptyAttributeSet();
        for(DMapNode nbr : node.nbrs) {
            nbrs.add(nbr.id);
        }
        return nbrs;
    }

    /**
     * Checks if x separates y from z
     * @param x
     * @param y
     * @param z
     * @return
     */
    public boolean separates(IAttributeSet x, IAttributeSet y, IAttributeSet z) {
        Queue<DMapNode> currentQueue = new LinkedList<DMapNode>();
        for(int i = y.nextAttribute(0); i >= 0; i = y.nextAttribute(i+1)) {
            currentQueue.add(vertices.get(i));
        }

        ArrayList<Integer> zIds = z.getIndices();
        ArrayList<Integer> xIds = x.getIndices();

        while(!currentQueue.isEmpty()) {
            DMapNode top = currentQueue.poll();
            for(DMapNode nbr: top.nbrs) {
                if(zIds.contains(nbr.id)) {
                    return true;
                } else if (!xIds.contains(nbr.id)){
                    currentQueue.add(nbr);
                }
            }
        }
        return false;
    }
}
