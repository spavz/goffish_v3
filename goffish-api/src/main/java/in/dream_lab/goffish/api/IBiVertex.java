package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;

import java.util.ArrayList;



/*
 * Created by vis on 5/7/17.
 */
public interface IBiVertex <V extends Writable, E extends Writable, I extends Writable, J extends Writable> extends IVertex<V, E, I, J> {

    Iterable<IEdge<E, I, J>> getInEdges();

}
