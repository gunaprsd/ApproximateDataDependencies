package uwdb.discovery.dependency.approximate.inference.dmap;

import java.util.ArrayList;

/**
 * Created by gunaprsd on 5/10/17.
 */
public class DMapNode {
    public Integer id;
    public ArrayList<DMapNode> nbrs;

    public DMapNode(int id) {
        this.id = id;
        this.nbrs = new ArrayList<DMapNode>();
    }
    public void connect(DMapNode b) {
        this.nbrs.add(b);
        b.nbrs.add(this);
    }
}
