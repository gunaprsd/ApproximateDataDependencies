package uwdb.discovery.dependency.approximate.inference.bayesianmap;


import java.util.ArrayList;

public class IMapDirectedNode {
    public Integer id;
    public ArrayList<IMapDirectedNode> parents;

    public IMapDirectedNode(Integer id) {
        this.id = id;
        this.parents = new ArrayList<>();
    }

    public void clear() {
        parents.clear();
    }
    public void addParent(IMapDirectedNode node) {
        parents.add(node);
    }
}
