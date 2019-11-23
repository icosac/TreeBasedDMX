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
import org.json.*;

import it.unitn.TBDMX.TBDMXNode.ImposeHolder;
import it.unitn.TBDMX.TBDMXNode.RequestCS;
import it.unitn.TBDMX.TBDMXNode.SetNeighbors;
import it.unitn.TBDMX.TBDMXNode.Advice;
import it.unitn.TBDMX.TBDMXNode.Restart;
import it.unitn.TBDMX.TBDMXNode.Privilege;
import it.unitn.TBDMX.TBDMXNode.Request;
import it.unitn.TBDMX.TBDMXNode.BroadcastHolder;

public class TBDMXController {
  final static int N_nodes = 5;

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("TBDMX");

    // Create all nodes of the system
    List<ActorRef> group = new ArrayList<>();
    for (int i=0; i<N_nodes; i++) {
      group.add(system.actorOf(TBDMXNode.props(i), "node" + i));
    }

    List<ArrayList<ActorRef>> neighbors = new ArrayList<>();
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(1))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(0),group.get(2))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(1),group.get(3))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(2),group.get(4))));
    neighbors.add(new ArrayList<ActorRef>(Arrays.asList(group.get(3))));


    // Send neighbors message
    for (int i=0; i<N_nodes; i++){
      SetNeighbors neighMessage = new SetNeighbors(neighbors.get(i));
      group.get(i).tell(neighMessage, null);
    }

    group.get(2).tell(new ImposeHolder(),null);

    try { Thread.sleep(1000); } 
    catch (InterruptedException e) { e.printStackTrace(); }

    group.get(0).tell(new RequestCS(2042), null);
    try { Thread.sleep(100); } 
    catch (InterruptedException e) { e.printStackTrace(); }
    group.get(1).tell(new RequestCS(2042), null);
    try { Thread.sleep(100); } 
    catch (InterruptedException e) { e.printStackTrace(); }
    group.get(3).tell(new RequestCS(2042), null);
    try { Thread.sleep(100); } 
    catch (InterruptedException e) { e.printStackTrace(); }
    group.get(0).tell(new RequestCS(2042), null);

    try {
      System.in.read();
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}
