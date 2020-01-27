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
  private boolean recoveryHolder;
  private boolean crashed;
  private boolean recovering;
  private boolean asked;
  private boolean using;
  private int adviceCounter;
  private List<Advice> receivedAdvices = new ArrayList<>();
  private ActorRef holderNode;
  private List<ActorRef> neighbors = new ArrayList<>();
  private Queue<ActorRef> requestQueue = new LinkedList<>();   
  private Queue<ActorRef> recoveryQueue = new LinkedList<>();   
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
        //piggybacking token request back 
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
    public final ActorRef sender;
    public final boolean holder; //"to me, you're the holder"
    public final boolean asked; //"I have asked the token"
    public final int adviceCounter;
    public final boolean inRequestQueue;
    public Advice(ActorRef sender, boolean holder, boolean asked, boolean inRequestQueue, int adviceCounter){
      this.sender = sender;
      this.holder = holder;
      this.asked = asked;
      this.inRequestQueue = inRequestQueue;
      this.adviceCounter = adviceCounter;
    }
    public String toString(){
      return ("this.sender: "+this.sender+" "+"this.holder: "+this.holder+" "+"this.asked: "+this.asked+" "+"this.inRequestQueue: "+this.inRequestQueue+" "+"this.adviceCounter: "+this.adviceCounter);
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
    log("Node "+this.id+"(holder: "+this.holder+"): requesting CS");
    this.time = msg.time;
    if (!this.crashed && !this.recovering){
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
    else if (this.recovering){
      log("Node "+this.id+": recovery requestCS enqueued");
      while (!this.recoveryQueue.offer(getSelf())); //assures to push on queue without exceptions
      log("Node "+this.id+": recoveryQueue is now "+this.recoveryQueue);     
    }
  }
  
  private void onBroadcastHolder(BroadcastHolder msg) {
    this.holder = false;
    this.holderNode = getSender();
    log("Node "+ this.id +": holderNode is "+this.holderNode);
    for (ActorRef node : this.neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
        log(this.id+": sending BroadcastHolder from "+ getSelf()+" to "+node);
      }
    }  
  }
  
  private void onRequest(Request msg) {
    log("Node "+this.id+"(using: "+this.using+", asked: "+this.asked+"): received request from "+getSender());
    if (!this.crashed && !this.recovering){
      addToRequestQueue(getSender());
      if (!this.using && !this.asked){
        if (this.holder){
          serveQueue();
          this.asked = false;
        } 
        else if (this.requestQueue.size()==1) { //not the holder, single element
          this.asked = true;
          this.holderNode.tell(new Request(),getSelf());
        }
      }
    } 
    else if (this.recovering){
      log("Node "+this.id+": recovery request enqueued");
      while (!this.recoveryQueue.offer(getSender())); //ensures to push on queue without exceptions
      log("Node "+this.id+": recoveryQueue is now "+this.recoveryQueue);
    }
  }
  
  private void onPrivilege(Privilege msg) {
    if (!this.crashed){
      this.adviceCounter = 0;
      log("Node "+this.id+": access granted by "+getSender());
      this.holder = true;
      this.asked = false;
      serveQueue();
    }
    else if(this.recovering){
      this.recoveryHolder = true;
    }
  }
  
  private void onRestart(Restart msg) {
    this.adviceCounter++;                         //Keep a counter to count the number of time I sent an advice to be sure not to go into starvation
    getSender().tell(                             //Send back an Advice message to whom asked for.
      new Advice(
        getSelf(),
        this.holderNode==getSender(),
        this.asked,
        this.requestQueue.contains(getSender()),
        this.adviceCounter
      ),
      getSelf()
    );
  }
  
  private void onAdvice(Advice msg) {
    log("Node "+this.id+" received advice from node: "+getSender()+" containing: "+msg.toString());
    this.receivedAdvices.add(msg);                                                          //Add all Advice messages to a queue
    if (receivedAdvices.size() == this.neighbors.size()) {                                  //When full start analyzes. For sure it will become full since no package can be lost.
      this.receivedAdvices.sort((Advice a1,Advice a2)->a1.adviceCounter-a2.adviceCounter);  //Sort all messages for the adviceCounter in order not to starve any node.
      for (Advice ad : receivedAdvices) {                                                   //For all the messages in the list
        System.out.println("Advice: "+ad.sender);
        if (ad.asked) {                                                                     //If it requsted me the token, then I add it to the queue.
          requestQueue.add(ad.sender);
        }

        this.holder &= ad.holder;                                                           //If all the nodes say that I'm the holder, then I'm the holder, otherwise someone else is
        if (!ad.holder) {
          this.holderNode = ad.sender;                                                      //Set the holder to that node
          this.asked = ad.inRequestQueue;                                                   //Set asked to true if I've made a request, that is I'm in its requestQueue
          log("Holder found again! "+ad.sender+" inrequestQueue: "+ad.inRequestQueue);
        }
        log("Node "+this.id+": During recovery: "+this.holder);
      }
      this.receivedAdvices.clear();
      
      if (this.recoveryHolder) {                                                            //If while I was crashed I received a Privilege message, then I'm the holder.
        this.adviceCounter = 0;
        log("Node "+this.id+": (recovery) access granted ");
        this.holder = true;
        this.asked = false;
      }
      while (!this.recoveryQueue.isEmpty()){                                                //If a received Request messages while I was recovering, then I add them at the end of the queue I created before hand
        this.requestQueue.add(this.recoveryQueue.remove());
      }
      
      if (this.holder){                                                                     //Finally, if I'm the holder, then I can serve the queue if it is not empty
        if (!this.requestQueue.isEmpty()){
          serveQueue();
          this.asked = false;
        }
      } 
      else if (!this.asked && !this.requestQueue.isEmpty()) {                                                               //Else I can send a Request message to my holderNode.
        this.asked = true;
        this.holderNode.tell(new Request(),getSelf());
      }
      this.recovering = false;
    }
    log("After crashing "+String.valueOf(this.requestQueue));
  }

  private void onCrash(Crash msg) {
    log("Node "+this.id+" crashed.");
    if (!this.crashed){                                                     //If I receive the crashed command, and my status is not crashed, then
      log("Before crashing "+String.valueOf(this.requestQueue));
      this.crashed = true;                                                  //Set my status to crashed
      this.requestQueue.clear();                                            //Clear the queue of requests
      this.holder = true;                                                   //I set optimistically the holder to my self, works best with the recovery phase
      this.asked = false;                                                   //I assume I've never asked
      this.holderNode = null;                                               //There is no holder since I assume it's me.
    }
    else {
      log("Node "+this.id+": node is already down!");
    }
  }

  private void onRecovery(Recovery msg) {
    log("Node "+this.id+" recovering.");
    if (this.crashed){ //If it crashed
      for (ActorRef node : neighbors) { //For all its neighbors
        node.tell(new Restart(), this.getSelf()); //It asks for infos
      }
      this.crashed = false; //After sending the message it is not in the crashed phase anymore
      this.recovering = true; //But it starts to recover.
    } 
    else {
      log("Node "+this.id+": node is up and runnning, nothing to recover!");
    }
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

