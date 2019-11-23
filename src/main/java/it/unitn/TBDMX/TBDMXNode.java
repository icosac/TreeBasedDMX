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

  private float time;
  
  /*-- Actor constructors --------------------------------------------------- */
  public TBDMXNode(int id) {
    this.id = id;
    System.out.println("Node "+id+" up.")
  }

  static public Props props(int id) {
    return Props.create(TBDMXNode.class, () -> new TBDMXNode(id));
  }

  /*-- Auxiliary functions--------------------------------------------------- */

  private void addToRequestQueue(ActorRef node) {
    while (!requestQueue.offer(node)); //assures to push on queue without exceptions
  }

  private void serveQueue() {
    if !(this.requestQueue.isEmpty()){
      TBDMXNode head = this.requestQueue.remove();
      if (head==getSelf()){
        criticalSection();
      }
      else {
        head.tell(new Privilege(),getSelf());
        if (!this.requestQueue.isEmpty()) {
          head.tell(new Request(),getSelf());
          asked = True;
        }
      }
    } 
    else {
      System.err.println("ERROR in serveQueue: trying to serve an empty queue")
    }
  }

  /*-- Message classes ------------------------------------------------------ */

  public static class ImposeHolder implements Serializable {}
  public static class SetNeighbors implements Serializable {
    public final List<ActorRef> group;
    public SetNeighbors(List<ActorRef> group) {
      this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));

  }
  public static class RequestCS implements Serializable {
    public final float time;
    public requestCS(float time){
      this.time = time;
    }
  }
  
  public static class BroadcastHolder implements Serializable {}
  public static class Request implements Serializable {}
  public static class Privilege implements Serializable {}
  public static class Restart implements Serializable {}
  public static class Advise implements Serializable {
    public final boolean holder; //"to me, you're the holder"
    public final boolean asked; //"I have asked the token"
    public final int requestCounter;
    public Advide(boolean holder, boolean asked, int requestCounter){
      this.holder = holder;
      this.asked = asked;
      this.requestCounter = requestCounter;
    }
  }
  
  private void onImposeHolder(ImposeHolder msg) {
    this.holder = True;
    for (ActorRef node : neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
      }
    }
  }
  
  private void onSetNeighbors(SetNeighbors msg) {
    this.neighbors = msg.group;
  }

  private void onRequestCS(RequestCS msg) {
    this.time = msg.time;
    if (!this.requestQueue.isEmpty()) {
      addToRequestQueue(getSelf());
    } 
    else if (!this.holder && this.requestQueue.isEmpty()) {
      addToRequestQueue(getSelf());
      holderNode.tell(new Request(),getSelf());
      asked = True;
    }
    else if (this.holder && this.requestQueue.isEmpty()) {
      this.using = True;
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
    this.holder = False;
    this.holderNode = getSender();
    
    for (ActorRef node : this.neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
      }
    }  
  }
  
  private void onRequest(Request msg) {
    addToRequestQueue(msg.getSender());
    if (!this.using and !this.asked){
      if (this.holder){
        getSender().tell(new Privilege(),getSelf());
        this.requestQueue.pop();
        this.holder = False;
        this.asked = False;
        this.holderNode = getSender();
      } 
      else if (this.requestQueue.size()==1) { //not the holder, single element
        this.asked = True;
        holderNode.tell(new Privilege(),getSelf());
      }
    }
  }
  
  private void onPrivilege(Privilege msg) {
    this.holder = True;
    this.asked = False;
    serveQueue();
  }
  
  private void onRestart(Restart msg) {

  }
  
  private void onAdvise(Advise msg) {

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
      .match(Advise.class,  this::onAdvise)
      .build();
  }
}
