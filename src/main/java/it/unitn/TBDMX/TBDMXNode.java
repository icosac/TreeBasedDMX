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
      System.err.println("Node "+this.id+": cannot initialize log! Probably missing directory `logs`");
    }
    log("Node "+id+" up.");
  }

  static public Props props(int id) {
    return Props.create(TBDMXNode.class, () -> new TBDMXNode(id));
  }

  /*-- Auxiliary functions--------------------------------------------------- */
  /**
   * Writes a string of log both in the log file of the node and on the console.
   *
   * @param      s     The string to be logged.
   */
  private void log(String s){
    try {
      this.logger.write(s+"\n");
    } 
    catch (IOException e) {
      System.err.println("Node "+this.id+": logging error");
    }
    System.out.println(s);
  }

  /**
   * Function that simulates a critical section. 
   * The thread just goes to sleep for a certain period of time. 
   */
  private void criticalSection(){
    this.using = true;
    log("Entering CS @ node "+this.id);
    try { Thread.sleep(this.time); } 
    catch (InterruptedException e) { e.printStackTrace(); }
    log("Exiting CS @ node "+this.id);
    this.using = false;
  }

  /**
   * Function to add a node to a `reqeustQueue`.
   * It first checks whether the node to be added is already inside the queue.
   * This method uses the `offer` function which ensures to push on queue without exceptions
   *
   * @param      node  The node to be added to the queue
   */
  private void addToRequestQueue(ActorRef node) {
    if (!this.requestQueue.contains(node)){
      while (!this.requestQueue.offer(node));
      log("Node "+this.id+": Queue is now "+this.requestQueue);
    }
    else {
      log("Node "+this.id+" is already in queue"+this.requestQueue);
    }

  }

  /**
   * A method to serve the next element in a non-empty queue. 
   * If the next element to be served is the node itself, then it enters the critical section. 
   * Once exited the CS, if the queue is not empty, being the possessor of the token, the node calls again the method. 
   * If instead it is one of the neighbors, a `Privilege` message is sent. 
   * Moreover if the queue is not empty after having served the neighbor, a `Request` message is "piggybacks" 
   * in order to obtain the token again and serve the next element in the queue.
   */
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
  
  /**
   * Function called upon receipt of an `ImposeHolder` message.
   * It sends a `BroadcastHolder` to all its neighbors. 
   *
   * @param      msg   The message indicating that the node is the initial holder.
   */
  private void onImposeHolder(ImposeHolder msg) {
    this.holder = true;
    for (ActorRef node : neighbors) {
      if (node != getSender()) {
        node.tell(new BroadcastHolder(),getSelf());
        log(this.id+": IS HOLDER. Sending BroadcastHolder from "+ getSelf()+" to "+node);
      }
    }
  }
  
  /**
   * Upon receiving a `SetNeighbors` message, the node sets its neighbors to the same value as the one in the message.
   *
   * @param      msg   The message containing the neighbors to be set.
   */
  private void onSetNeighbors(SetNeighbors msg) {
    this.neighbors = msg.group;
  }

  /**
   * Called on 'RequestCS' which is sent from the controller indicating that the node should enter the critical section.
   * If the node is not crashed nor recovering, then it checks whether its `requestQueue` is empty or not. If it is not, then it adds itself to the queue.
   * If instead the queue is empty, then it checks whether it is the holder of the token. If it is, then it enters the critical section.
   * If it is not the holder, then it adds itself to the queue and sets `asked` to `true` in order to remember having sent a request.
   * If the node is recovering while receiving the request, it adds such request to a secondary queue, `recoveryQueue` which will be merged with `serveQueue` later.
   *
   * @param      msg   The message containing how much time the node should stay inside the CS.
   */
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
      while (!this.recoveryQueue.offer(getSelf())); //ensures to push on queue without exceptions
      log("Node "+this.id+": recoveryQueue is now "+this.recoveryQueue);     
    }
  }
  
  /**
   * Called upon receiving a `BroadcastHolder` message. The node receiving the message will set it's `holderNode` to the sender of the message, 
   * and the `holder` flag to `false` since it is not the holder.
   * Then it sends a new a `BroadcastHolder` to each neighbor, except the sender of the previous `BroadcastHolder`. 
   *
   * @param      msg   The message indicating to store and broadcast information on the holder.
   */
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
  
  /**
   * Upon receiving a `Request` message, if the node is not crashed nor recovering, then it adds the request to the queue. 
   * It then checks whether it is not in the critical section nor it has already asked for the token, and if it is the holder, it then serves the queue. 
   * If the node added to the queue is the only node in the queue, then no Request can have been sent before to request the token, hence one should be sent.
   *
   * @param      msg   A message requiring the token.
   */
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
  
  /**
   * Called on the `Privilege` message receipt. If the node is not crashed nor is recovering, then it received the token correctly and can serve the next element in the queue.
   * At this moment it also reset the counter `adviceCounter` since it got the token and is not risking starvation anymore.
   * If instead the node is recovering, then the flag `recoveryHolder` is set, which indicates that during the recovery a `Privilege` message was received, but the queue could not be served. 
   *
   * @param      msg   The message (virtually) bringing the token.
   */
  private void onPrivilege(Privilege msg) {
    if (!this.crashed && !this.recovering){
      this.adviceCounter = 0;
      log("Node "+this.id+": access granted by "+getSender());
      this.holderNode = getSelf();
      this.holder = true;
      this.asked = false;
      serveQueue();
    }
    else if(this.recovering){
      this.recoveryHolder = true;
    }
  }
  
  /**
   * Upon receiving a `Restart` message, the receiver sends an `Advice` message to the sender of the `Restart` which had crashed.
   * In particular, the `Advice` message, will tell the crashed node:
   * if it was the holder wrt to the actual node;
   * if the actual node made any request to satisfy any previous request;
   * if the actual node contains the crashed node in its queue;
   * A counter that allows avoiding starvation of the node.
   *
   * @param      msg   The `Restart` message indicating a crashed node and the need for information.
   */
  private void onRestart(Restart msg) {
    this.adviceCounter++;                         
    getSender().tell(                             
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
  
  /**
   * Upon receiving an `Advice` message, the node stores such message. When it received an `Advice` from each neighbor, then it can start the recover the information it had before crashing.
   * First of all the messages are sort by `adviceCounter` to avoid starvation. Then, if 
   * - All `Advice`s indicate that the crashed node was the holder, then it is the holder of the token.
   * - One of the nodes states that the crashed node was not the holder, then it was not, but the first node is the `holderNode` for the crashed one.
   * - One of the nodes states that the crashed nose was the `holderNode` wrt to it, and it made a request to the crashed node, then such node should be added to the requestQueue.
   *  Then the node checks whether something happened during its recovery phase. If the `recoveryHolder` the crashed node is the holder of the token. If the `recoveryQueue` is not empty,
   *  then some requests have arrived before recovering completely and they should be added to the `requestQueue`. 
   *  At last if the node is now the holder, then it can proceed to serve the next node in the queue. If instead it is not the holder and its queue is not empty and it hasn't make any request yet, 
   *  it will send a `Request` to its holder.
   *
   * @param      msg   The message containing information about the node before the crash.
   */
  private void onAdvice(Advice msg) {
    log("Node "+this.id+" received advice from node: "+getSender()+" containing: "+msg.toString());
    this.receivedAdvices.add(msg);                                                          //Add all Advice messages to a queue
    if (receivedAdvices.size() == this.neighbors.size()) {                                  //When full start analyzes. For sure it will become full since no package can be lost.
      this.receivedAdvices.sort((Advice a1,Advice a2)->a1.adviceCounter-a2.adviceCounter);  //Sort all messages for the adviceCounter in order not to starve any node.
      for (Advice ad : receivedAdvices) {                                                   //For all the messages in the list
        System.out.println("Advice: "+ad.sender);
        if (ad.asked && ad.holder) {                                                        //If it requsted me the token, then I add it to the queue.
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
        ActorRef recoveredNode=this.recoveryQueue.remove();
        if (!this.requestQueue.contains(recoveredNode)) {
          this.requestQueue.add(recoveredNode);
        }
      }
      
      if (this.holder){                                                                    //Finally, if I'm the holder, then I can serve the queue if it is not empty
        if (!this.requestQueue.isEmpty()){
          serveQueue();
          this.asked = false;
        }
      } 
      else if (!this.asked && !this.requestQueue.isEmpty()) {                              //Else I can send a Request message to my holderNode.
        this.asked = true;
        this.holderNode.tell(new Request(),getSelf());
      }
      this.recovering = false;
      log("After crashing "+String.valueOf(this.requestQueue));
    }
  }

  /**
   * Upon receiving a `Crash` message, if the node was not already crashed or recovering, then it crashes, which implies the lose of knowledge regarding the token and the request. 
   * The list of neighbors is preserved.
   *
   * @param      msg   The message which dooms a node.
   */
  private void onCrash(Crash msg) {
    log("Node "+this.id+" crashed.");
    if (!this.crashed && !this.recovering){                                 //If I receive the crashed command, and my status is not crashed, then
      log("Before crashing "+String.valueOf(this.requestQueue));
      this.crashed = true;                                                  //Set my status to crashed
      this.requestQueue.clear();                                            //Clear the queue of requests
      this.holder = true;                                                   //I set optimistically the holder to my self, works best with the recovery phase
      this.asked = false;                                                   //I assume I've never asked
      this.holderNode = null;                                               //There is no holder since I assume it's me.
    }
    else {
      log("Node "+this.id+": node is already down or recovering!");
    }
  }

  /**
   * Upon receipt of a `Recovery` message, if the token is still crashed, then it starts the recovery procedure, 
   * which implies as a first step to send a `Restart` message to all the neighbors to gather information
   *
   * @param      msg   A message that tells to a crashed node to reboot.
   */
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

  /**
   * When the controller sends such message to the nodes, it indicates that they need to close the buffer onto which they were writing their log.
   *
   * @param      msg   The message indicating to close the logs.
   */
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

