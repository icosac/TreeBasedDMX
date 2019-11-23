package it.unitn.TBDMX;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.util.Collections;


public class TBDMXNode extends AbstractActor {
  private int id; // node ID
  private boolean holder;
  private boolean asked;
  private boolean using;
  private ActorRef holderNode;
  private List<ActorRef> neighbors = new ArrayList<>();
  private Queue<ActorRef> requestQueue = new LinkedList<>();   
  private Random rnd = new Random();

  private long time;
  
  /*-- Actor constructors --------------------------------------------------- */
  public TBDMXNode(int id) {
    this.id = id;
    System.out.println("Node "+id+" up.");
  }

  static public Props props(int id) {
    return Props.create(TBDMXNode.class, () -> new TBDMXNode(id));
  }

  /*-- Auxiliary functions--------------------------------------------------- */

  private void criticalSection(){
    this.using = true;
    System.out.println("Entering CS @ node "+this.id);
    try { Thread.sleep(this.time); } 
    catch (InterruptedException e) { e.printStackTrace(); }
    System.out.println("Exiting CS @ node "+this.id);
    this.using = false;
  }

  private void addToRequestQueue(ActorRef node) {
    while (!this.requestQueue.offer(node)); //assures to push on queue without exceptions
    System.out.println("Node "+this.id+": Queue is now "+this.requestQueue);
  }

  private void serveQueue() {
    if (!this.requestQueue.isEmpty()){
      ActorRef head = this.requestQueue.remove();
      System.out.println("Node "+this.id+": serving "+head+". Queue is now "+this.requestQueue);
      if (head==getSelf()){
        criticalSection();
      }
      else {
        head.tell(new Privilege(),getSelf());
        this.holder = false;
        this.holderNode = head;
        if (!this.requestQueue.isEmpty()) {
          head.tell(new Request(),getSelf());
          asked = true;
        }
      }
    } 
    else {
      System.err.println("ERROR in serveQueue (node "+this.id+"): trying to serve an empty queue");
    }
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class ImposeHolder implements Serializable {}
  public static class SetNeighbors implements Serializable {
    public final List<ActorRef> group;
    public SetNeighbors(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
    }
  }
  public static class RequestCS implements Serializable {
    public final long time;
    public RequestCS(long time){
      this.time = time;
    }
  }
  
  public static class BroadcastHolder implements Serializable {}
  public static class Request implements Serializable {}
  public static class Privilege implements Serializable {}
  public static class Restart implements Serializable {}
  public static class Advice implements Serializable {
    public final boolean holder; //"to me, you're the holder"
    public final boolean asked; //"I have asked the token"
    public final int requestCounter;
    public Advice(boolean holder, boolean asked, int requestCounter){
      this.holder = holder;
      this.asked = asked;
      this.requestCounter = requestCounter;
    }
  }
  
  private void onImposeHolder(ImposeHolder msg) {
    this.holder = true;
    for (ActorRef node : neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
        System.out.println(this.id+": IS HOLDER. Sending BroadcastHolder from "+ getSelf()+" to "+node);
      }
    }
  }
  
  private void onSetNeighbors(SetNeighbors msg) {
    this.neighbors = msg.group;
  }

  private void onRequestCS(RequestCS msg) {
    System.out.print("Node "+this.id+"("+this.holder+"): requesting CS");
    this.time = msg.time;
    if (!this.requestQueue.isEmpty()) {
      addToRequestQueue(getSelf());
    } 
    else if (!this.holder && this.requestQueue.isEmpty()) {
      addToRequestQueue(getSelf());
      this.holderNode.tell(new Request(),getSelf());
      this.asked = true;
    }
    else if (this.holder && this.requestQueue.isEmpty()) {
      criticalSection();
      if (!this.requestQueue.isEmpty()) {
        serveQueue();          
      }
    }  
    else {
      System.err.println("ERROR in RequestCS: impossible node state");
      System.exit(-1);
    }
  }
  
  private void onBroadcastHolder(BroadcastHolder msg) {
    this.holder = false;
    this.holderNode = getSender();
    System.out.println("Node "+ this.id +": holdernode is "+this.holderNode);
    for (ActorRef node : this.neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
        System.out.println(this.id+": sending BroadcastHolder from "+ getSelf()+" to "+node);
      }
    }  
  }
  
  private void onRequest(Request msg) {
    System.out.println("Node "+this.id+"("+this.using+","+this.asked+"): received request from "+getSender());
    addToRequestQueue(getSender());
    if (!this.using && !this.asked){
      if (this.holder){
        serveQueue();
        this.holder = false;
        this.asked = false;
        this.holderNode = getSender();
      } 
      else if (this.requestQueue.size()==1) { //not the holder, single element
        this.asked = true;
        holderNode.tell(new Request(),getSelf());
      }
    }
  }
  
  private void onPrivilege(Privilege msg) {
    System.out.println("Node "+this.id+": access granted by "+getSender());
    this.holder = true;
    this.asked = false;
    serveQueue();
  }
  
  private void onRestart(Restart msg) {

  }
  
  private void onAdvice(Advice msg) {

  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(ImposeHolder.class,  this::onImposeHolder)
      .match(SetNeighbors.class,  this::onSetNeighbors)
      .match(RequestCS.class,  this::onRequestCS)
      .match(BroadcastHolder.class,  this::onBroadcastHolder)
      .match(Request.class,  this::onRequest)
      .match(Privilege.class,  this::onPrivilege)
      .match(Restart.class,  this::onRestart)
      .match(Advice.class,  this::onAdvice)
      .build();
  }
}
