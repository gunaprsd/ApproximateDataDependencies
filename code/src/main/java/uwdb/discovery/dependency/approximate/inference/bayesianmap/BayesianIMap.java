package uwdb.discovery.dependency.approximate.inference.bayesianmap;


import org.apache.commons.lang3.StringUtils;
import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;
import uwdb.discovery.dependency.approximate.interfaces.IDataset;

import java.io.PrintStream;
import java.util.*;

public class BayesianIMap {
    protected RelationSchema schema;
    protected IAttributeSet subset;
    protected HashMap<Integer, IMapDirectedNode> vertices;
    protected IDataset dataset;
    ArrayList<Integer> vertexOrdering;
    protected ArrayList<IMapNode> undirectedVertices;
    protected ArrayList<IAttributeSet> keys;

    public BayesianIMap(RelationSchema schema, IAttributeSet subsetAttributes, IDataset dataset) {
        this.schema = schema;
        this.dataset = dataset;
        this.vertices = new HashMap<Integer, IMapDirectedNode>();
        this.subset = subsetAttributes;
    }

    public BayesianIMap(RelationSchema schema, IDataset dataset) {
        this.schema = schema;
        this.dataset = dataset;
        this.vertices = new HashMap<Integer, IMapDirectedNode>();
        this.subset = schema.getEmptyAttributeSet();
        for(int i = 0; i < schema.getNumAttributes(); i++) {
            this.subset.add(i);
        }
    }

    public void initialize()
    {
        for(int i = subset.nextAttribute(0); i >= 0; i = subset.nextAttribute(i+1)) {
            vertices.put(i, new IMapDirectedNode(i));
        }
        this.keys = new ArrayList<>();
    }

    public void computeBayesianIMap(ArrayList<Integer> vertexOrdering) {
        this.vertexOrdering = vertexOrdering;
        IAttributeSet potentialParentsSet = schema.getEmptyAttributeSet();
        for(int i = 0; i < vertices.size(); i++) {
            int idx = vertexOrdering.get(i);
            if(!potentialParentsSet.isEmpty()) {
                //find minimal subset such that I(X_i, B_i, U_i - B_i - X_i) holds!
                IAttributeSet minimalSubset = findMinimalSubset(idx, potentialParentsSet);
                //IAttributeSet minimalSubset = findMinimalSubset(idx, schema.getEmptyAttributeSet(), potentialParentsSet);
                System.out.printf("vertex : %d, parentSet : %s\n", idx, minimalSubset);
                for(int j = minimalSubset.nextAttribute(0); j >= 0; j = minimalSubset.nextAttribute(j+1)) {
                    this.vertices.get(idx).addParent(this.vertices.get(j));
                }
            }
            potentialParentsSet.add(idx);
        }
    }

    public void computeBayesianIMap() {
        System.out.printf("Total entropy : %f\n", dataset.computeEntropy(schema.getEmptyAttributeSet().complement()));
        ArrayList<Integer> vertexOrdering = schema.getEmptyAttributeSet().complement().getIndices();
        computeBayesianIMapGreedily(vertexOrdering);
    }

    protected void computeBayesianIMapGreedily(ArrayList<Integer> vertexOrdering) {

        IAttributeSet potentialParentsSet = schema.getEmptyAttributeSet();
        for(int i = 0; i < vertexOrdering.size(); i++) {
            int idx = vertexOrdering.get(i);
            if(!potentialParentsSet.isEmpty()) {
                IAttributeSet minimalSubset = findMinimalSubset(idx, potentialParentsSet);
                System.out.printf("vertex : %d, parentSet : %s\n", idx, minimalSubset);
                IAttributeSet minimalSubsetForm = minimalSubset.clone();
                minimalSubsetForm.add(idx);
                if(isKey(minimalSubsetForm) && i != vertexOrdering.size() - 1) {
                    System.out.printf("minimal subset is key : %s\n", minimalSubset);
                    keys.add(minimalSubset);
                    vertexOrdering.removeAll(minimalSubset.getIndices());
                    vertexOrdering.addAll(vertexOrdering.size(), minimalSubset.getIndices());
                    clear();
                    computeBayesianIMapGreedily(vertexOrdering);
                    return;
                } else {
                    System.out.printf("minimal subset + idx entropy : %s\n", dataset.computeEntropy(minimalSubsetForm));
                }

                for(int j = minimalSubset.nextAttribute(0); j >= 0; j = minimalSubset.nextAttribute(j+1)) {
                    this.vertices.get(idx).addParent(this.vertices.get(j));
                }
            }
            potentialParentsSet.add(idx);
            this.vertexOrdering = vertexOrdering;
        }
    }

    protected void clear() {
        for(int i = 0; i < vertices.size(); i++) {
            vertices.get(i).clear();
        }
    }

    protected boolean isKey(IAttributeSet set) {
        return Math.abs(dataset.computeEntropy(set) - dataset.computeEntropy(schema.getEmptyAttributeSet().complement())) < 1E-6;
    }

    public void computeBayesianIMapGreedyOrdering() {
        this.vertexOrdering = new ArrayList<>();
        computeBayesianIMapGreedyOrdering(schema.getEmptyAttributeSet().complement());
    }

    protected void computeBayesianIMapGreedyOrdering(IAttributeSet subset) {
        System.out.printf("Computing for %s\n", subset);
        int max_idx = -1;
        int max_cardinality = Integer.MIN_VALUE;
        IAttributeSet max_parentSet = null;
        double max_entropy = Double.MAX_VALUE;

        for(int i = subset.nextAttribute(0); i >= 0; i = subset.nextAttribute(i+1)) {
            IAttributeSet potentialParentSet = subset.clone();
            potentialParentSet.remove(i);

            IAttributeSet parentSet = findMinimalSubset(i, potentialParentSet);
            System.out.printf("Parent set for %d is %s\n", i, parentSet);
            if(parentSet.cardinality() > max_cardinality) {
                max_idx = i;
                max_cardinality = parentSet.cardinality();
                max_parentSet = parentSet;
                max_entropy = dataset.computeEntropy(schema.getAttributeSet(i));
            } else if(parentSet.cardinality() == max_cardinality) {
                //choose the one with maximum entropy
                double entropy = dataset.computeEntropy(schema.getAttributeSet(i));
                if(entropy > max_entropy) {
                    max_idx = i;
                    max_cardinality = parentSet.cardinality();
                    max_parentSet = parentSet;
                    max_entropy = entropy;
                }
            }
        }

        System.out.printf("Choosing %d\n", max_idx);
        subset.remove(max_idx);
        vertexOrdering.add(0, max_idx);
        //adding parents for min_idx
        for(int j = max_parentSet.nextAttribute(0); j >= 0; j = max_parentSet.nextAttribute(j+1)) {
            this.vertices.get(max_idx).addParent(this.vertices.get(j));
        }

        if(!subset.isEmpty()) {
            computeBayesianIMapGreedyOrdering(subset);
        }
    }

    protected boolean checkMVD(Integer vertexId, IAttributeSet parentSubset, IAttributeSet porentialParentSet) {
        //Z
        IAttributeSet B_i = parentSubset;
        //YZ
        IAttributeSet U_i = porentialParentSet;
        //XZ
        IAttributeSet B_i_X_i = B_i.clone();
        B_i_X_i.add(vertexId);

        //XYZ
        IAttributeSet U_i_X_i = porentialParentSet.clone();
        U_i_X_i.add(vertexId);

        double ent_Z = dataset.computeEntropy(B_i);
        double ent_YZ = dataset.computeEntropy(U_i);
        double ent_XZ = dataset.computeEntropy(B_i_X_i);
        double ent_XYZ = dataset.computeEntropy(U_i_X_i);

        double mf = ent_XZ + ent_YZ - ent_XYZ - ent_Z;

        if(mf < -1E-7) {
            //System.out.printf("mutual information cannot be negative : %f!\n", mf);
        } else if(mf < 1E-6) {
            //almost zero. so it holds
            //System.out.printf("I({%d}; %s|%s) holds!\n", vertexId, U_i.minus(B_i), B_i);
            return true;
        }
        return false;
    }

    //improvised version
    protected IAttributeSet findMinimalSubset(Integer vertexId, IAttributeSet potentialParentSet) {
        IAttributeSet S = potentialParentSet.clone();

        IAttributeSet SX = potentialParentSet.clone();
        SX.add(vertexId);

        double entropyValue = dataset.computeEntropy(SX) - dataset.computeEntropy(S);

        for(int i = potentialParentSet.nextAttribute(0); i >= 0; i = potentialParentSet.nextAttribute(i+1)) {
            IAttributeSet S_minus_v = S.clone();
            S_minus_v.remove(i);

            IAttributeSet SX_minus_v = S_minus_v.clone();
            SX_minus_v.add(vertexId);

            double newEntropyValue = dataset.computeEntropy(SX_minus_v) - dataset.computeEntropy(S_minus_v);

            if(Math.abs(newEntropyValue - entropyValue) <= 1E-7) {
                S.remove(i);
                if(!checkMVD(vertexId, S_minus_v, potentialParentSet)) {
                    System.out.printf("Entropy values: %f, %f\n", entropyValue, newEntropyValue);
                    System.out.println("Wrong conclusion!");
                }
            }
        }

        return S;
    }

    protected IAttributeSet findMinimalSubset(Integer vertexId, IAttributeSet subset, IAttributeSet potentialParentSet) {
        if(checkMVD(vertexId, subset, potentialParentSet)) {
            return subset;
        } else {
            IAttributeSet notInSubset = potentialParentSet.minus(subset);
            for(int i = notInSubset.nextAttribute(0); i >= 0; i =  notInSubset.nextAttribute(i+1)) {
                IAttributeSet nextLevelSubset = subset.clone();
                nextLevelSubset.add(i);
                IAttributeSet result = findMinimalSubset(vertexId, nextLevelSubset, potentialParentSet);
                if(result == null) {
                    continue;
                } else {
                    return result;
                }
            }
        }
        //should not reach here!
        return null;
    }

    public void moralize() {
        this.undirectedVertices = new ArrayList<>();
        for(int i = 0; i < schema.getNumAttributes(); i++) {
            undirectedVertices.add(new IMapNode(i));
        }
        //connect to all parents
        for(int i = 0; i < schema.getNumAttributes(); i++) {
            IMapDirectedNode dnode = vertices.get(i);
            IMapNode unode = undirectedVertices.get(i);

            //connect parents to unode
            for( IMapDirectedNode pdnode : dnode.parents) {
                IMapNode punode = undirectedVertices.get(pdnode.id);
                unode.connect(punode);
            }

            //connecting parents together
            for( IMapDirectedNode pdnode1 : dnode.parents) {
                IMapNode punode1 = undirectedVertices.get(pdnode1.id);
                for( IMapDirectedNode pdnode2 : dnode.parents) {
                    IMapNode punode2 = undirectedVertices.get(pdnode2.id);
                    if(punode1 != punode2) {
                        punode1.connect(punode2);
                    }
                }
            }
        }
    }

    public void print()
    {
        String order = StringUtils.join(vertexOrdering, ", ");
        System.out.printf("Vertex Ordering: {%s}\n", order);
        for(Integer idx : vertexOrdering) {
            IMapDirectedNode n = vertices.get(idx);
            System.out.printf("%s : ", schema.getAttributeString(n.id));
            for(IMapDirectedNode nbr : n.parents) {
                System.out.printf("%s, ", schema.getAttributeString(nbr.id));
            }
            System.out.println();
        }
    }

    public void printMoralizedGraph() {
        String order = StringUtils.join(vertexOrdering, ", ");
        System.out.printf("Moralized Bayesian I-map for Vertex Ordering: {%s}\n", order);
        for(IMapNode n : undirectedVertices) {
            System.out.printf("%d:[", n.id);
            Collections.sort(n.nbrs);
            for(IMapNode nbr : n.nbrs) {
                System.out.printf("%d, ", nbr.id);
            }
            System.out.printf("]");
            System.out.println();
        }
    }

    public void printMoralizedGraphAdjacencyMatrix(PrintStream out, String id) {
        String order = StringUtils.join(vertexOrdering, ", ");
        System.out.printf("Ordering {%s} in file {%s}\n", order, id);
        for(IMapNode n : undirectedVertices) {
            Collections.sort(n.nbrs);
            StringBuilder row = new StringBuilder();
            for(int i = 0; i < schema.getNumAttributes(); i++) {
                if(n.nbrs.contains(undirectedVertices.get(i))) {
                   row.append("1, ");
                } else {
                    row.append("0, ");
                }
            }
            out.println(row);
        }
    }

    ArrayList<IAttributeSet> maximalCliques;

    public void findCliques() {
        maximalCliques = new ArrayList<>();
        BonKerboschAlgorithm(schema.getEmptyAttributeSet(), schema.getEmptyAttributeSet().complement(), schema.getEmptyAttributeSet());

        for(int i = 0; i < maximalCliques.size(); i++) {
            System.out.printf("%s\n", schema.getAttributeSetString(maximalCliques.get(i)));
        }
    }

    protected void BonKerboschAlgorithm(IAttributeSet R, IAttributeSet P, IAttributeSet X) {
        if(P.isEmpty() && X.isEmpty()) {
            maximalCliques.add(R);
        }

        for(int i = P.nextAttribute(0); i >= 0; i = P.nextAttribute(i+1)) {
            IMapNode node = undirectedVertices.get(i);
            IAttributeSet nbrs = schema.getEmptyAttributeSet();
            for(IMapNode nbrNode : node.nbrs) {
                nbrs.add(nbrNode.id);
            }
            IAttributeSet newR = R.clone(); newR.add(i);
            IAttributeSet newP = P.intersect(nbrs);
            IAttributeSet newX = X.intersect(nbrs);
            BonKerboschAlgorithm(newR, newP, newX);

            P.remove(i);
            X.add(i);
        }
    }


}
