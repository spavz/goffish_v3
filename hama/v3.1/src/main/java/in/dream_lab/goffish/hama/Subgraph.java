/**
 *  Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License. You may obtain
 *  a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  @author Himanshu Sharma
 *  @author Diptanshu Kakwani
 *  @author Vishak KC
*/

package in.dream_lab.goffish.hama;

import java.util.*;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;

public class Subgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements ISubgraph<S, V, E, I, J, K> {

  K subgraphID;
  private List<IEdge<E, I, J>> _interiorEdges;
  private List<IEdge<E, I, J>> _boundaryEdges;
  private Map<I, IVertex<V, E, I, J>> _interiorVertexMap;
  private Map<I, IVertex<V, E, I, J>> _boundaryVertexMap;
  private Map<I, IRemoteVertex<V, E, I, J, K>> _remoteVertexMap;
  int partitionID;
  S _value;

  Subgraph(int partitionID, K subgraphID) {
    this.partitionID = partitionID;
    this.subgraphID = subgraphID;
    _interiorVertexMap = new HashMap<I, IVertex<V, E, I, J>>();
    _boundaryVertexMap = new HashMap<I, IVertex<V, E, I, J>>();
    _remoteVertexMap = new HashMap<I, IRemoteVertex<V, E, I, J, K>>();
    _interiorEdges = new ArrayList<>();
    _boundaryEdges = new ArrayList<>();
  }

  void addVertex(IVertex<V, E, I, J> v) {
    if (v instanceof IRemoteVertex)
      _remoteVertexMap.put(v.getVertexId(), (IRemoteVertex<V, E, I, J, K>) v);
    else
      _interiorVertexMap.put(v.getVertexId(), v);
  }

  void addVertex(IVertex<V, E, I, J> v, boolean isBoundary) {
    if (isBoundary)
      _boundaryVertexMap.put(v.getVertexId(), v);
    else
      _interiorVertexMap.put(v.getVertexId(), v);
  }

  @Override
  public IVertex<V, E, I, J> getVertexById(I vertexID) {
    if(_interiorVertexMap.get(vertexID) != null)
      return  _interiorVertexMap.get(vertexID);
    else if(_boundaryVertexMap.get(vertexID) != null)
      return  _boundaryVertexMap.get(vertexID);
    return _remoteVertexMap.get(vertexID);
  }

  @Override
  public K getSubgraphId() {
    return subgraphID;
  }

  @Override
  public long getVertexCount() {
    return _interiorVertexMap.size() + _boundaryVertexMap.size() + _remoteVertexMap.size() ;
  }

  @Override
  public long getLocalVertexCount() {
    return _interiorVertexMap.size() + _boundaryVertexMap.size();
  }

  @Override
  public Iterable<IVertex<V, E, I, J>> getVertices() {
    return new Iterable<IVertex<V, E, I, J>>() {

      private Iterator<IVertex<V, E, I, J>> interiorVertexIterator = _interiorVertexMap.values().iterator();
      private Iterator<IVertex<V, E, I, J>> boundaryVertexIterator = _boundaryVertexMap.values().iterator();
      private Iterator<IRemoteVertex<V, E, I, J, K>> remoteVertexIterator = _remoteVertexMap.values().iterator();

      @Override
      public Iterator<IVertex<V, E, I, J>> iterator() {
        return new Iterator<IVertex<V, E, I, J>>() {
          @Override
          public boolean hasNext() {
            return interiorVertexIterator.hasNext() || boundaryVertexIterator.hasNext() || remoteVertexIterator.hasNext();
          }

          @Override
          public IVertex<V, E, I, J> next() {
            if (interiorVertexIterator.hasNext())
              return interiorVertexIterator.next();
            else if (boundaryVertexIterator.hasNext())
              return boundaryVertexIterator.next();
            return remoteVertexIterator.next();
          }

          @Override
          public void remove() {

          }
        };
      }
    };
  }

  @Override
  public Iterable<IVertex<V, E, I, J>> getLocalVertices() {
    return new Iterable<IVertex<V, E, I, J>>() {

      private Iterator<IVertex<V, E, I, J>> interiorVertexIterator = _interiorVertexMap.values().iterator();
      private Iterator<IVertex<V, E, I, J>> boundaryVertexIterator = _boundaryVertexMap.values().iterator();

      @Override
      public Iterator<IVertex<V, E, I, J>> iterator() {
        return new Iterator<IVertex<V, E, I, J>>() {
          @Override
          public boolean hasNext() {
            return interiorVertexIterator.hasNext() || boundaryVertexIterator.hasNext();
          }

          @Override
          public IVertex<V, E, I, J> next() {
            if (interiorVertexIterator.hasNext())
              return interiorVertexIterator.next();
            return boundaryVertexIterator.next();
          }

          @Override
          public void remove() {

          }
        };
      }
    };
  }

  @Override
  public void setSubgraphValue(S value) {
    _value = value;
  }

  @Override
  public S getSubgraphValue() {
    return _value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices() {
    return _remoteVertexMap.values();
  }

  @Override
  public IEdge<E, I, J> getEdgeById(J edgeID) {
    for(IEdge<E, I, J> e: _interiorEdges)
      if (edgeID.equals(e.getEdgeId()))
        return e;
    for(IEdge<E, I, J> e: _boundaryEdges)
      if (edgeID.equals(e.getEdgeId()))
        return e;
    return null;
  }

  @Override
  public Iterable<IEdge<E, I, J>> getOutEdges() {
    return new Iterable<IEdge<E, I, J>>() {


      private Iterator<IEdge<E, I, J>> interiorIterator = _interiorEdges.iterator();
      private Iterator<IEdge<E, I, J>> boundaryIterator = _boundaryEdges.iterator();

      @Override
      public Iterator<IEdge<E, I, J>> iterator() {
        return new Iterator<IEdge<E, I, J>>() {
          @Override
          public boolean hasNext() {
            return interiorIterator.hasNext() || boundaryIterator.hasNext();
          }

          @Override
          public IEdge<E, I, J> next() {
            if (interiorIterator.hasNext())
              return interiorIterator.next();
            return boundaryIterator.next();
          }

          @Override
          public void remove() {

          }
        };
      }
    };

  }

  public Iterable<IEdge<E, I, J>> getEdges(long iOff, long bOff, int degree) {
    return new Iterable<IEdge<E, I, J>>() {


      private Iterator<IEdge<E, I, J>> interiorIterator = _interiorEdges.listIterator((int) iOff);
      private Iterator<IEdge<E, I, J>> boundaryIterator = _boundaryEdges.listIterator((int) bOff);

      @Override
      public Iterator<IEdge<E, I, J>> iterator() {
        return new Iterator<IEdge<E, I, J>>() {
          int d = degree;

          @Override
          public boolean hasNext() {
            return d > 0 && (interiorIterator.hasNext()  || boundaryIterator.hasNext()) ;
          }

          @Override
          public IEdge<E, I, J> next() {
            d--;
            if (interiorIterator.hasNext())
              return interiorIterator.next();
            return boundaryIterator.next();
          }

          @Override
          public void remove() {

          }
        };
      }
    };

  }

  public Iterable<IEdge<E, I, J>> getInteriorEdges(long off, final int degree ) {
    return new Iterable<IEdge<E, I, J>>() {


      private Iterator<IEdge<E, I, J>> interiorIterator = _interiorEdges.listIterator((int) off);

      @Override
      public Iterator<IEdge<E, I, J>> iterator() {
        return new Iterator<IEdge<E, I, J>>() {
          int d = degree;
          @Override
          public boolean hasNext() {
            return interiorIterator.hasNext() && d > 0;
          }

          @Override
          public IEdge<E, I, J> next() {
              d--;
              return interiorIterator.next();
          }

          @Override
          public void remove() {

          }
        };
      }
    };

  }

  public Iterable<IEdge<E, I, J>> getBoundaryEdges(long off, final int degree ) {
    return new Iterable<IEdge<E, I, J>>() {


      private Iterator<IEdge<E, I, J>> interiorIterator = _boundaryEdges.listIterator((int) off);

      @Override
      public Iterator<IEdge<E, I, J>> iterator() {
        return new Iterator<IEdge<E, I, J>>() {
          int d = degree;
          @Override
          public boolean hasNext() {
            return interiorIterator.hasNext() && d > 0;
          }

          @Override
          public IEdge<E, I, J> next() {
            d--;
            return interiorIterator.next();
          }

          @Override
          public void remove() {

          }
        };
      }
    };

  }

  //@Override
  public long getRemoteVertexCount() {
    return _remoteVertexMap.size();
  }

  //@Override
  public long getLocalEdgeCount() {
    return _interiorEdges.size() + _boundaryEdges.size();
  }


  public long getInteriorEdgeCount() {
    return _interiorEdges.size();
  }


  //Override
  public long getBoundaryEdgeCount() {
    return _boundaryEdges.size();
  }

 // @Override
  public long getBoundaryVertexCount() {
   return _boundaryVertexMap.size();
  }


  //@Override
  public Iterable<IVertex<V,E,I,J>> getBoundaryVertices() {
    return _boundaryVertexMap.values();
  }


  //@Override
  public Iterable<IEdge<E, I, J>> getBoundaryEdges() {
    return _boundaryEdges;
  }

  public void addInteriorEdge(IEdge<E, I, J> e){
    _interiorEdges.add(e);
  }

  public void addBoundaryEdge(IEdge<E, I, J> e){
    _boundaryEdges.add(e);
  }

}
