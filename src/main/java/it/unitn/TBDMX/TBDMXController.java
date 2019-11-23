package it.unitn.TBDMX;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.lang.Thread;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;

import it.unitn.TBDMX.TBDMXNode.ImposeHolder;
import it.unitn.TBDMX.TBDMXNode.RequestCS;
import it.unitn.TBDMX.TBDMXNode.SetNeighbors;
import it.unitn.TBDMX.TBDMXNode.Advice;
import it.unitn.TBDMX.TBDMXNode.Restart;
import it.unitn.TBDMX.TBDMXNode.Privilege;
import it.unitn.TBDMX.TBDMXNode.Request;
import it.unitn.TBDMX.TBDMXNode.BroadcastHolder;
import it.unitn.TBDMX.TBDMXNode.SaveLog;

public class TBDMXController {
  final static int N_nodes =6 ;

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("TBDMX");

    // Create all nodes of the system
    List<ActorRef> group = new ArrayList<>();
    for (int i=0; i<N_nodes; i++) {
      group.add(system.actorOf(TBDMXNode.props(i), "node" + i));
    }

    List<ArrayList<ActorRef>> neighbors = new ArrayList<>();
    BufferedReader br = null;
    try { br = new BufferedReader(new FileReader("tree.conf")); }
    catch (FileNotFoundException e) {System.err.println("Tree file not found!"); System.exit(-2);}
    String st;
    try {
      while ((st = br.readLine()) != null) {
        ArrayList<ActorRef> nodeNeighbors = new ArrayList<>();
        for (String nId : st.split(" ",0)) {
          nodeNeighbors.add(group.get(Integer.parseInt(nId)));
        }
        neighbors.add(nodeNeighbors);
      }
    } catch(IOException e) {System.err.println("Error reading tree file!"); System.exit(-2);}
    /*neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(1))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(0),group.get(2))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(1),group.get(3))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(2),group.get(4))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(3))));
    */

    // Send neighbors message
    for (int i=0; i<N_nodes; i++){
      SetNeighbors neighMessage = new SetNeighbors(neighbors.get(i));
      group.get(i).tell(neighMessage, null);
    }

    group.get(2).tell(new ImposeHolder(),null);

    try { Thread.sleep(1000); } 
    catch (InterruptedException e) { e.printStackTrace(); }

    group.get(0).tell(new RequestCS(2042), null);
    group.get(1).tell(new RequestCS(2042), null);
    group.get(3).tell(new RequestCS(2042), null);
    group.get(0).tell(new RequestCS(2042), null);
    group.get(5).tell(new RequestCS(2042), null);

    try {
      System.in.read();
      for (int i=0; i<N_nodes; i++){
        group.get(i).tell(new SaveLog(), null);
      }
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}
