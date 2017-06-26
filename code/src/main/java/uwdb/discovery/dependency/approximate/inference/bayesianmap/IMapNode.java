package uwdb.discovery.dependency.approximate.inference.bayesianmap;

import uwdb.discovery.dependency.approximate.inference.dmap.DMapNode;
import uwdb.discovery.dependency.approximate.interfaces.IAttributeSet;

import java.util.ArrayList;

public class IMapNode implements Comparable<IMapNode> {
    public Integer id;
    public ArrayList<IMapNode> nbrs;
    public boolean isPartOfClique;

    public IMapNode(int id) {
        this.id = id;
        this.nbrs = new ArrayList<IMapNode>();
        this.isPartOfClique = false;
    }
    public void connect(IMapNode b) {
        if(!this.nbrs.contains(b)) {
            this.nbrs.add(b);
        }

        if(!b.nbrs.contains(this)) {
            b.nbrs.add(this);
        }
    }

    public void clear() {
        nbrs.clear();
    }

    @Override
    public int compareTo(IMapNode o) {
        return id - o.id;
    }
}
