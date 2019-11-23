package it.unitn.TBDMX;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.json.*;

import it.unitn.TBDMX.TBDMXNode.ImposeHolder;
import it.unitn.TBDMX.TBDMXNode.RequestCS;
import it.unitn.TBDMX.TBDMXNode.SetNeighbors;
import it.unitn.TBDMX.TBDMXNode.Advise;
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

    List<List<ActorRef>> neighbors = new ArrayList<>();
    neighbors.add(new ArrayList<ActorRef>(group.get(1)))
    neighbors.add(new ArrayList<ActorRef>(group.get(0),group.get(2)))
    neighbors.add(new ArrayList<ActorRef>(group.get(1),group.get(3)))
    neighbors.add(new ArrayList<ActorRef>(group.get(2),group.get(4)))
    neighbors.add(new ArrayList<ActorRef>(group.get(3)))


    // Send neighbors message
    for (int i=0; i<N_nodes; i++){
      SetNeighbors neighMessage = new SetNeighbors(neighbors.get(i));
      group.get(i).tell(neighMessage, null);
    }

    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}
