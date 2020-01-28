package it.unitn.TBDMX;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.Array;
import scala.concurrent.duration.Duration;

import java.io.*;
import java.lang.Thread;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  /**
   * Function that reads the structure of the tree from `tree.conf`. There are two possible configurations:
   * 1. The n-th line contains the neighbors for the n-th node separated by a space.
   * 2. An xml file that contains a list (`<nodes>`) of `<node>`s and a list (`<edges>`) of `<edge>`s that is connections between one node and another.
   * No flag needs to be set since it checks at the beginning of the file the presence of an xml tag. If no tag is found, then it assumes that lists of neighbors will be used, otherwise the xml file.
   * The only requirement is that in case of using the xml file, it need to be well formatted, that is one node per line and one edge per line.
   *
   * @param      neighbors  The neighbors to then be passed to the `ActoRef`s.
   */
  private static void readFromFile(List<ArrayList<Integer>> neighbors){
    BufferedReader br=null;
    String st;
    try {
      br = new BufferedReader(new FileReader(new File("tree.conf")));
    }
    catch (Exception e) {System.err.println("Tree file not found!"); System.exit(-2);}
    try {
      st = br.readLine();
      if (Pattern.compile(".*xml.*").matcher(st).find()) {
        Pattern pattNode = Pattern.compile(".*node.*mainText=\"([0-9]+)\".*");
        Pattern pattEdge = Pattern.compile(".*vertex1=\"([0-9]+)\".*vertex2=\"([0-9]+)\".*");
        List<Integer> nodes = new ArrayList<>();
        while ((st = br.readLine()) != null) {
          Matcher m = pattNode.matcher(st);
          if (m.find()) {
            nodes.add(Integer.parseInt(m.group(1)));
            neighbors.add(new ArrayList<>());
          } else {
            m = pattEdge.matcher(st);
            if (m.find()) {
              neighbors.get(Integer.parseInt(m.group(1))).add(Integer.parseInt(m.group(2)));
              neighbors.get(Integer.parseInt(m.group(2))).add(Integer.parseInt(m.group(1)));
            }
          }
        }
      } else {
        do {
          ArrayList<Integer> nodeNeighborsIds = new ArrayList<>();
          for (String nId : st.split(" ", 0)) {
            nodeNeighborsIds.add(Integer.parseInt(nId));
          }
          neighbors.add(nodeNeighborsIds);
        } while ((st = br.readLine()) != null);
      }
    }
    catch(IOException e) {System.err.println("Error reading tree file!"); System.exit(-2);}
  }

  public static void main(String[] args) {
    // Create the actor system
    final ActorSystem system = ActorSystem.create("TBDMX");

    // Parse tree structure
    List<ArrayList<Integer>> neighborsIds = new ArrayList<>();
    readFromFile(neighborsIds);
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

    //Read commands from file commands.conf
    BufferedReader commandsbr = null;
    String st;
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
