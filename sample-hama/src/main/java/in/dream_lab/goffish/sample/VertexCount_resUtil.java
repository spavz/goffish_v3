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
*/

package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.List;

/**
 * Prints the total number of vertices in the graph across all nodes
 * 
 * @author humus
 *
 */

public class VertexCount_resUtil extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

  long t1, t2, count;

  @Override
  public void compute(Iterable<IMessage<LongWritable, LongWritable>> messages)
      throws IOException {
    if (getSuperstep() == 0) {
      long count = getSubgraph().getLocalVertexCount();
      LongWritable message = new LongWritable(count);
      sendToAll(message);

    } else {

      long totalVertices = 0;
      for (IMessage<LongWritable, LongWritable> msg : messages) {
        LongWritable count = msg.getMessage();
        totalVertices += count.get();
      }
      ISubgraph<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
      System.out.println("Total vertices = " + totalVertices);

//      t1 = System.nanoTime();
//      count = subgraph.getLocalEdgeCount();
//      t2 = System.nanoTime();
//      System.out.print("getLocalEdgeCount: " + (t2-t1));

      t1 = System.nanoTime();
      count = subgraph.getVertexCount();
      t2 = System.nanoTime();
      System.out.print("getVertexCount: " + (t2-t1));


      t1 = System.nanoTime();
      Iterable<IVertex<LongWritable,LongWritable,LongWritable,LongWritable>> l=subgraph.getLocalVertices();
      t2 = System.nanoTime();
      System.out.print("getLocalVertices: " + (t2-t1));

      t1 = System.nanoTime();
      Iterable<IEdge<LongWritable, LongWritable, LongWritable>> Edges = subgraph.getOutEdges();
      t2 = System.nanoTime();
      System.out.print("getOutEdges: " + (t2-t1));

      t1 = System.nanoTime();
      long boundaryEdge=0;
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> localVertex : subgraph.getLocalVertices())
        for(IEdge<LongWritable, LongWritable, LongWritable> temp : localVertex.getOutEdges())
          boundaryEdge++;

        t2 = System.nanoTime();

        System.out.print("getBoundaryEdges() Karan: " + (t2-t1));


      t1 = System.nanoTime();
      Iterable<IVertex<LongWritable,LongWritable,LongWritable,LongWritable>> b =subgraph.getBoundaryVertices();
      t2 = System.nanoTime();
      System.out.print("getBoundaryEdges() Efficient : " + (t2-t1));





        try {
        // Use OutputWriter
      } finally {

      }
    }
    voteToHalt();
  }

}
