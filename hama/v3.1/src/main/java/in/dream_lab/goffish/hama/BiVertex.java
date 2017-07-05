package in.dream_lab.goffish.hama;

import in.dream_lab.goffish.api.IBiVertex;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vis on 5/7/17.
 */
public class BiVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable>
        extends Vertex<V,E,I,J>
        implements IBiVertex<V,E,I,J> {

    private List<IEdge<E, I, J>> _inadjList;

    BiVertex(I Id, Iterable<IEdge<E, I, J>> edges) {
        super(Id,edges);
        _inadjList = new ArrayList<>();

    }

    @Override
    public Iterable<IEdge<E, I, J>> getInEdges() {
        return _inadjList;
    }

    public void addInEdge(IEdge<E, I, J> e) {
        _inadjList.add(e);
    }

    public void addAllInEdges(ArrayList<IEdge<E, I, J>> edges) {
        _inadjList.addAll(edges);
    }
}
