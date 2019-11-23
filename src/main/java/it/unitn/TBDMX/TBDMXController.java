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
import it.unitn.TBDMX.TBDMXNode.Recovery;
import it.unitn.TBDMX.TBDMXNode.Crash;

public class TBDMXController {
  static int N_nodes;

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("TBDMX");

    // Parse tree structure
    List<ArrayList<Integer>> neighborsIds = new ArrayList<>();
    BufferedReader treebr = null;
    try { treebr = new BufferedReader(new FileReader("tree.conf")); }
    catch (FileNotFoundException e) {System.err.println("Tree file not found!"); System.exit(-2);}
    String st;
    try {
      while ((st = treebr.readLine()) != null) {
        ArrayList<Integer> nodeNeighborsIds = new ArrayList<>();
        for (String nId : st.split(" ",0)) {
          nodeNeighborsIds.add(Integer.parseInt(nId));
        }
        neighborsIds.add(nodeNeighborsIds);
      }
    } catch(IOException e) {System.err.println("Error reading tree file!"); System.exit(-2);}
    N_nodes = neighborsIds.size();

    // Create all nodes of the system
    List<ActorRef> group = new ArrayList<>();
    for (int i=0; i<N_nodes; i++) {
      group.add(system.actorOf(TBDMXNode.props(i), "node" + i));
    }
    List<ArrayList<ActorRef>> neighbors = new ArrayList<>();
    for (int i=0; i<N_nodes; i++) {
      ArrayList<ActorRef> nodeNeighbors = new ArrayList<>();
      for (Integer neighborId : neighborsIds.get(i)){
        nodeNeighbors.add(group.get(neighborId));
      }
      neighbors.add(nodeNeighbors);
    }
 
    // Send neighbors message
    for (int i=0; i<N_nodes; i++){
      SetNeighbors neighMessage = new SetNeighbors(neighbors.get(i));
      group.get(i).tell(neighMessage, null);
    }


    BufferedReader commandsbr = null;
    try { commandsbr = new BufferedReader(new FileReader("commands.conf")); }
    catch (FileNotFoundException e) {System.err.println("Command file not found!"); System.exit(-2);}
    try {
      boolean firstline = true;
      while ((st = commandsbr.readLine()) != null) {
        if (firstline) {
          group.get(Integer.parseInt(st)).tell(new ImposeHolder(),null);
          firstline = false;
          try { Thread.sleep(1000); } 
          catch (InterruptedException e) { e.printStackTrace(); }
        }
        else {
          ArrayList<String> command = new ArrayList<> (Arrays.asList(st.split(" ",0)));
          System.out.println(command);
          if (command.get(0).equals("request")) {
            group.get(Integer.parseInt(command.get(1))).tell(new RequestCS(Integer.parseInt(command.get(2))), null);            
          }
          else if (command.get(0).equals("crash")) {
            group.get(Integer.parseInt(command.get(1))).tell(new Crash(), null);
          }
          else if (command.get(0).equals("recovery")) {
            group.get(Integer.parseInt(command.get(1))).tell(new Recovery(), null);
          }
          else if (command.get(0).equals("wait")) {
            try { Thread.sleep(Integer.parseInt(command.get(1))); } 
            catch (InterruptedException e) { e.printStackTrace(); }
          }
        }
      }
    } catch(IOException e) {System.err.println("Error reading command file!"); System.exit(-2);}

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
