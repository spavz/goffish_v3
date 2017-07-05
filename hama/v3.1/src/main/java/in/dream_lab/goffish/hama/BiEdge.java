package in.dream_lab.goffish.hama;

import in.dream_lab.goffish.api.IBiEdge;
import org.apache.hadoop.io.Writable;

/**
 * Created by vis on 5/7/17.
 */
public class BiEdge<E extends Writable, I extends Writable, J extends Writable>
    extends Edge<E,I,J> implements IBiEdge<E,I,J> {

    private I _source;

    BiEdge(J id, I sinkID) {
        super(id, sinkID);
    }

    BiEdge(I sourceID, J id, I sinkID) {
        this(id, sinkID);
        _source = sourceID;
    }

    @Override
    public I getSourceVertexId() {
        return _source;
    }
}
