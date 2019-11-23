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
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TBDMXNode extends AbstractActor {
  private int id; // node ID
  private boolean holder;
  private boolean asked;
  private boolean using;
  private ActorRef holderNode;
  private List<ActorRef> neighbors = new ArrayList<>();
  private Queue<ActorRef> requestQueue = new LinkedList<>();   
  private Random rnd = new Random();
  private BufferedWriter logger;

  private long time;
  
  /*-- Actor constructors --------------------------------------------------- */
  public TBDMXNode(int id) {
    this.id = id;
    try {
      this.logger = new BufferedWriter(new FileWriter("logs/node"+id+".log"));
    }
    catch (IOException e){
      System.err.println("Node "+this.id+": cannot initialize log!");
    }
    log("Node "+id+" up.");
  }

  static public Props props(int id) {
    return Props.create(TBDMXNode.class, () -> new TBDMXNode(id));
  }

  /*-- Auxiliary functions--------------------------------------------------- */

  private void log(String s){
    try {
      this.logger.write(s+"\n");
    } 
    catch (IOException e) {
      System.err.println("Node "+this.id+": logging error");
    }
    System.out.println(s);
  }

  private void criticalSection(){
    this.using = true;
    log("Entering CS @ node "+this.id);
    try { Thread.sleep(this.time); } 
    catch (InterruptedException e) { e.printStackTrace(); }
    log("Exiting CS @ node "+this.id);
    this.using = false;
  }

  private void addToRequestQueue(ActorRef node) {
    while (!this.requestQueue.offer(node)); //assures to push on queue without exceptions
    log("Node "+this.id+": Queue is now "+this.requestQueue);
  }

  private void serveQueue() {
    if (!this.requestQueue.isEmpty()){
      ActorRef head = this.requestQueue.remove();
      log("Node "+this.id+": serving "+head+". Queue is now "+this.requestQueue);
      if (head==getSelf()){
        criticalSection();
        if (!this.requestQueue.isEmpty()) { 
          serveQueue();
        }    
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
  
  public static class SaveLog implements Serializable {}
  public static class Crash implements Serializable {}
  public static class Recovery implements Serializable {}
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
        log(this.id+": IS HOLDER. Sending BroadcastHolder from "+ getSelf()+" to "+node);
      }
    }
  }
  
  private void onSetNeighbors(SetNeighbors msg) {
    this.neighbors = msg.group;
  }

  private void onRequestCS(RequestCS msg) {
    log("Node "+this.id+"("+this.holder+"): requesting CS");
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
    log("Node "+ this.id +": holdernode is "+this.holderNode);
    for (ActorRef node : this.neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
        log(this.id+": sending BroadcastHolder from "+ getSelf()+" to "+node);
      }
    }  
  }
  
  private void onRequest(Request msg) {
    log("Node "+this.id+"("+this.using+","+this.asked+"): received request from "+getSender());
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
    log("Node "+this.id+": access granted by "+getSender());
    this.holder = true;
    this.asked = false;
    serveQueue();
  }
  
  private void onRestart(Restart msg) {

  }
  
  private void onAdvice(Advice msg) {

  }

  private void onCrash(Crash msg) {

  }

  private void onRecovery(Recovery msg) {

  }

  private void onSaveLog(SaveLog msg){
    try {
      this.logger.close();
      System.out.println("Node "+this.id+": log saved successfully.");
    }
    catch (IOException e) {
      System.err.println("Error closing log file.");
    }
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(SaveLog.class,  this::onSaveLog)
      .match(Crash.class,  this::onCrash)
      .match(Recovery.class,  this::onRecovery)
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
