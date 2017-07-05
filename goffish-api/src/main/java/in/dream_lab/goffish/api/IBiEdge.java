package in.dream_lab.goffish.api;

import org.apache.hadoop.io.Writable;

/*
 * Created by vis on 5/7/17.
 * @param <I> Represents source and sink Vertex type.
 */
public interface IBiEdge<E extends Writable, I extends Writable, J extends Writable> extends IEdge<E,I,J> {

    I getSourceVertexId();

}
