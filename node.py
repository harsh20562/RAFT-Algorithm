"""
TODO:
- Check if leader's lease is running only then GetVal & SetVal requests are entertained.
- If there is NO leader, then GetVal & SetVal requests are not entertained.
"""

import sys
import random as rnd
import json
from helper import *
import grpc
import raft_pb2_grpc as raft_grpc
import raft_pb2 as raft
import time
from threading import Timer, Event, Lock, Thread
from concurrent import futures
import signal
import ast

"""
├─ logs_node_x/
│  ├─ logs.txt
│  ├─ metadata.txt
│  ├─ dump.txt
"""

"""
0 -> FOLLOWER
1 -> CANDIDATE
2 -> LEADER
"""

timer_lock = Event()
term_lock = Lock()
state_lock = Lock()
election_lock = Lock()
voting_lock = Lock()

IS_LEADER = False
ELECTION_T1, ELECTION_T2 = 5, 15

class Node:
  """
    TODO: Store all variables in JSON format and reload from it in __init__
  """
  def __init__(self, id: int, address: Node_Address, neighbours):
    self.state = 0 
    self.term = 0 
    self.id = id
    self.address = address
    self.neighbours = neighbours
    self.last_vote_term = -1
    self.leader_id = self.id
    self.commitIndex = 0
    self.lastApplied = 0
    self.log_table = []
    self.nextIndex = []
    self.matchIndex = []
    self.applied_entries = {}
    self.votedForTerm = []
    self.local_lock = Lock()
    self.prevLeaseTime = 0.0
    self.lease_time = 6.0
    self.IsLeaseAcquired = False
    self.hasLeader = False
    self.counter = 0

    self.dir_path = f"logs/logs_node_{self.id}"
    self.log_path = f"{self.dir_path}/logs.txt"
    self.metadata_path = f"{self.dir_path}/metadata.txt"
    self.dump_path = f"{self.dir_path}/dump.txt"
    self.txt_dump = f"{self.dir_path}/data.txt"
    create_dir(self.dir_path)
    create_file(self.log_path)
    create_file(self.metadata_path)
    create_file(self.dump_path)
    create_file(self.txt_dump)
    self.load_data()
    msg = f"NO-OP {self.term}"
    write_to_file(self.log_path, msg)
    self.start()

  def save_data(self):
    with open(self.txt_dump, 'w') as f:
      attributes_to_save = ["state", "term", "id", "last_vote_term", "leader_id", "commitIndex", "lastApplied", "log_table", "nextIndex", "matchIndex", "votedForTerm", "applied_entries", "prevLeaseTime"]
      for attr in attributes_to_save:
          value = getattr(self, attr)
          if isinstance(value, list):
              value = ','.join(map(str, value))
          elif isinstance(value, dict):
              value = ','.join(f'{k}:{v}' for k, v in value.items())
          elif attr == "state":
              value = "0"
          f.write(f'{attr}:{value}\n')
  
  def load_data(self):
    ints = ['state', 'term', 'id', 'last_vote_term', 'leader_id', 'commitIndex', 'lastApplied']
    lists = ['nextIndex', 'matchIndex', 'votedForTerm']
    with open(self.txt_dump, 'r') as f:
      for line in f:
        key, value = line.strip().split(':', 1)
        if value == '':
            continue
        if key == 'log_table':
            entries = value.strip('{}').split('},{')
            value = []
            for entry in entries:
                entry_dict = ast.literal_eval('{' + entry + '}')
                value.append(entry_dict)
        elif key == 'applied_entries':
            value = dict(item.split(':') for item in value.split(','))
        elif key in lists:
            value = list(map(int, value.split(',')))
        elif key in ints:
            value = int(value)
            if key == 'state':
              value = 0
        elif key == 'prevLeaseTime':
            value = float(value)
        setattr(self, key, value)

  def start(self):
    self.initialize_timer()
    self.timer.start()
  
  def initialize_timer(self):
    # self.timer_interval = self.id * 2 + 5
    self.timer_interval = rnd.uniform(ELECTION_T1, ELECTION_T2)
    self.timer = Timer(self.timer_interval, self.follower_timer)
  
  def follower_timer(self):
    if self.state == 0 and self.timer.finished:
      with election_lock:
        self.become_candidate()
        self.start_election()
  
  def reinitialize_timer(self):
    # self.timer_interval = self.id * 2 + 5
    self.timer_interval = rnd.uniform(ELECTION_T1, ELECTION_T2)

  def reset_timer(self):
    self.timer.cancel()
    self.timer = Timer(self.timer_interval, self.follower_timer)
    self.timer.start()

  def update_term(self):
    with term_lock:
        self.term += 1

  def update_state(self, state: int):
    with state_lock:
      self.state = state

  def update_vote(self, term: int):
    if self.last_vote_term < term:
      self.last_vote_term = term
  
  def become_candidate(self):
    self.update_state(1)
    self.update_term()
  
  def start_election(self):
    """Collect votes from neighbours and itself.
    Becomes Leader if possible.
    """
    if self.state != 1:
      return
    
    self.hasLeader = False

    global IS_LEADER
    IS_LEADER = False
    if IS_LEADER == True:
      IS_LEADER = False
      msg = f"{self.leader_id} Stepping down"
      write_to_file(self.dump_path, msg)
      print(msg)
    
    msg = f"Node {self.id} election timer timed out, Starting election."
    write_to_file(self.dump_path, msg)
    print(msg)

    # with voting_lock:
    requests = []
    votes = [0 for _ in range(len(self.neighbours))]
    prevLeaseTimeArray = [0 for _ in range(len(self.neighbours))]
    for n in self.neighbours:
      if n.id != self.id:
        thread = Thread(target=self.request_vote, args=(n, votes, prevLeaseTimeArray))
        requests.append(thread)
        thread.start()
      else:
        time.sleep(2)
        with self.local_lock:
          if self.term not in self.votedForTerm:
            votes[self.id] = 1
            msg = f"Vote granted for Node {self.id} in term {self.term}."
            write_to_file(self.dump_path, msg)
            print(msg)

    for thread in requests:
      thread.join()
    
    if self.state != 1:
      return

    if sum(votes) > len(votes) // 2:
      self.become_leader(max(prevLeaseTimeArray))
    else:
      self.update_state(0)
      self.reinitialize_timer()
      self.reset_timer()
    
  def request_vote(self, addr: Node_Address, votes, prevLeaseTimeArray):
    if self.state != 1:
      return
    channel = grpc.insecure_channel(f"{addr.ip}:{addr.port}")
    stub = raft_grpc.RaftServiceStub(channel)
    request = {
      'candidateTerm': self.term,
      'candidateId': self.id,
      'lastLogIndex': self.lastApplied,
      'lastLogTerm': len(self.log_table) - 1
    }
    try:
      response = stub.RequestVote(raft.RequestVoteRequest(**request))
      if response.term > self.term:
        self.term = response.term
        self.become_follower()
      elif response.result and response.term <= self.term:
        votes[addr.id] = 1
        prevLeaseTimeArray[addr.id] = response.prevLeaseTime
    except Exception as e:
      pass
  
  def become_follower(self):
    self.update_state(0)
    self.reset_timer()

  def become_leader(self, prevMaxLeaseTime = 0):
    self.nextIndex = [len(self.log_table)+1] * len(self.neighbours)
    self.matchIndex = [0] * len(self.neighbours)

    if self.state == 1:

      self.hasLeader = True
      self.lease_time = 6.0

      global IS_LEADER
      IS_LEADER = True
      msg = f"Node {self.id} became the leader for term {self.term}."
      write_to_file(self.dump_path, msg)
      print(msg)
      msg = f"NO-OP {self.term}"
      write_to_file(self.log_path, msg)

      """
        - [DONE] Wait for the prevMaxLeaseTime to expire
        - Once expired, send heartbeat with self.lease_time
      """

      # print(f"PrevMaxLeaseTime: {prevMaxLeaseTime}")

      self.IsLeaseAcquired = False

      start_time = time.time()
      while time.time() - start_time < prevMaxLeaseTime:
        self.heartbeat_lease_timer()
        msg = f"New Leader waiting for Old Leader Lease to timeout."
        write_to_file(self.dump_path, msg)
        print(msg)
      
      """
        - In all the heartbeats sent from now, the remaining lease time of the leader is also sent.
      """

      self.IsLeaseAcquired = True

      self.update_state(2)
      self.heartbeat_timer()
      self.leader_id = self.id


  def heartbeat_lease_timer(self):
    if self.state != 2:
      return
    pool = []
    
    self.save_data()

    for n in self.neighbours:
      if n.id != MY_ADDR.id:
        thread = Thread(target=self.send_heartbeat, args=(n,))
        thread.start()
        pool.append(thread)

    for t in pool:
      t.join()

    self.nextIndex[self.id] = len(self.log_table) + 1
    self.matchIndex[self.id] = len(self.log_table) 

    counter = 0
    for element in self.matchIndex:
      if element >= self.commitIndex + 1: # collecting votes
          counter += 1

    majority = len(self.neighbours) // 2
    if counter > majority:
      self.commitIndex += 1
    while self.commitIndex > self.lastApplied:
      key = self.log_table[self.lastApplied]['update'][1]
      value = self.log_table[self.lastApplied]['update'][2]
      self.applied_entries[key] = value
      self.lastApplied += 1
      msg = f"Node {self.id} (leader) committed the entry {key} {value} to the state machine."
      write_to_file(self.dump_path, msg)
      print(msg)
  
  def heartbeat_timer(self):
    if self.state != 2:
      return
    
    self.lease_start_time = time.time()

    if (time.time() - self.lease_start_time) > self.lease_time:
      msg = f"Leader {self.id} lease renewal failed. Stepping Down."
      self.IsLeaseAcquired = False
      write_to_file(self.dump_path, msg)
      print(msg)
      self.become_follower()
      self.reset_timer()
      return
    pool = []

    self.save_data()

    for n in self.neighbours:
      if n.id != MY_ADDR.id:
        thread = Thread(target=self.send_heartbeat, args=(n,))
        thread.start()
        pool.append(thread)

    for t in pool:
      t.join()

    self.nextIndex[self.id] = len(self.log_table) + 1
    self.matchIndex[self.id] = len(self.log_table) 

    counter = 0
    for element in self.matchIndex:
      if element >= self.commitIndex + 1: # collecting votes
          counter += 1

    majority = len(self.neighbours) // 2
    if counter > majority:
      msg = f"Leader {self.id} sending heartbeat & Renewing Lease"
      write_to_file(self.dump_path, msg)
      print(msg)
      self.commitIndex += 1
      self.lease_start_time = time.time()
    while self.commitIndex > self.lastApplied:
      key = self.log_table[self.lastApplied]['update'][1]
      value = self.log_table[self.lastApplied]['update'][2]
      self.applied_entries[key] = value
      self.lastApplied += 1
      msg = f"Node {self.id} (leader) committed the entry {key} {value} to the state machine."
      write_to_file(self.dump_path, msg)
      print(msg)

    if counter > majority:
      self.lease_start_time = time.time()

    self.leader_timer = Timer(50 / 1000, self.heartbeat_timer)
    self.leader_timer.start()

  def send_heartbeat(self, addr):
    channel = grpc.insecure_channel(f"{addr.ip}:{addr.port}")
    stub = raft_grpc.RaftServiceStub(channel)

    prevLogIndex = self.nextIndex[addr.id] - 1
    prevLogTerm = 0
    entries = []

    if self.nextIndex[addr.id] <= len(self.log_table):
      entries = [self.log_table[self.nextIndex[addr.id]-1]]
    if self.nextIndex[addr.id] > 1:
      prevLogTerm = self.log_table[self.nextIndex[addr.id]-2]['term']

    request = {
      'leaderTerm': self.term,
      'leaderId': self.id,
      'prevLogIndex': prevLogIndex, # WARNING may result in an error change it to -1
      'prevLogTerm': prevLogTerm, # same as above
      'entries': entries,
      'leaderCommit': self.commitIndex,
      'remainingLeaseTime': self.lease_time - (time.time() - self.lease_start_time)
    }

    # msg = f"Node {self.id} (leader) sent AppendEntries RPC to Node {addr.id} with entries {entries}."
    # print(msg)

    try:
      response = stub.AppendEntries(
        raft.AppendEntriesRequest(**request)
      )
      if response.term > self.term:
        self.term = response.term
        self.update_state(0)
      else:
        if response.success:
          if self.nextIndex[addr.id] <= len(self.log_table):
            self.matchIndex[addr.id] = self.nextIndex[addr.id]
            self.nextIndex[addr.id] += 1
        else:
          self.nextIndex[addr.id] -= 1
          self.matchIndex[addr.id] = min(self.matchIndex[addr.id], self.nextIndex[addr.id] - 1)
              
    except Exception as e:
      msg = f"Error occurred while sending RPC to Node {addr.id}."
      write_to_file(self.dump_path, msg)
      # print(msg)

class RaftServiceHandler(raft_grpc.RaftServiceServicer, Node):
  def __init__(self, id: int, address: Node_Address, neighbours):
    super().__init__(id, address, neighbours)
    print(f"Node starts at {address.ip}:{address.port}")
  
  def RequestVote(self, request, context):
    candidate_term = request.candidateTerm
    candidate_id = request.candidateId
    candidate_lastLogIndex = request.lastLogIndex
    candidate_lastLogTerm = request.lastLogTerm
    result = True

    with self.local_lock:
      if self.term in self.votedForTerm:
        result = False

    if candidate_term < self.term:
      result = False
    if self.last_vote_term >= candidate_term:
      result = False
    if candidate_lastLogIndex < len(self.log_table):
      result = False
    if candidate_lastLogIndex == len(self.log_table):
      if len(self.log_table) > 0:
        if self.log_table[-1]['term'] != candidate_lastLogIndex:
          result = False

    if result and candidate_id != MY_ADDR.id:
      self.become_follower()
      self.reset_timer()
      self.leader_id = candidate_id

    self.counter = 0

    if result:
      with self.local_lock:
        self.votedForTerm.append(self.term)
      msg = f"commitIndex: {self.commitIndex}, term: {self.term}, NodeID: {candidate_id}"
      write_to_file(self.metadata_path, msg)
      msg = f"Vote granted for Node {candidate_id} in term {self.term}."
      write_to_file(self.dump_path, msg)
      print(msg)
    else:
      msg = f"Vote denied for Node {candidate_id} in term {self.term}."
      write_to_file(self.dump_path, msg)
      print(msg)

    self.hasLeader = False

    response = {
      'term': self.term,
      'result': result,
      'prevLeaseTime': self.prevLeaseTime
    }
    return raft.RequestVoteResponse(**response)

  def AppendEntries(self, request, context):
    term = request.leaderTerm
    leader_id = request.leaderId
    prevLogIndex = request.prevLogIndex
    prevLogTerm = request.prevLogTerm
    entries = request.entries # This is a list of one entry, easier to handle
    leaderCommit = request.leaderCommit
    success = True if term >= self.term else False # first condition

    self.counter += 1

    if self.counter == 1:
      msg = f"NO-OP {term}"
      write_to_file(self.log_path, msg)

    self.prevLeaseTime = request.remainingLeaseTime

    if term > self.term:
      self.term = term
      self.become_follower()
      self.leader_id = leader_id
    
    if prevLogIndex > len(self.log_table): # second condition
      success = False
    
    if prevLogIndex <= len(self.log_table): # third condition, conflict
      self.log_table = self.log_table[:prevLogIndex]
            
    if len(entries) > 0: # Not a hearbeat 
      entry = entries[0]
      entry_term = entry.term
      entry_update = entry.update
      entry_flattened = {
        'term': entry_term,
        'update': entry_update
      }
      if entry_flattened not in self.log_table:
        self.log_table.append(entry_flattened)
    

    if success:
      self.hasLeader = True
      msg = f"Node {self.id} accepted AppendEntries RPC from {self.leader_id}."
      write_to_file(self.dump_path, msg)
      # print(msg)
    else:
      msg = f"Node {self.id} rejected AppendEntries RPC from {self.leader_id}."
      write_to_file(self.dump_path, msg)
      # print(msg)


    if leaderCommit > self.commitIndex:
      self.commitIndex = min(leaderCommit, len(self.log_table))
      while self.commitIndex > self.lastApplied:
        key = self.log_table[self.lastApplied]['update'][1]
        value = self.log_table[self.lastApplied]['update'][2]
        self.applied_entries[key] = value
        self.lastApplied += 1
        msg1 = f"Node {self.id} (follower) committed the entry {key} {value} to the state machine."
        write_to_file(self.dump_path, msg1)
        print(msg1)
        msg2 = f"SET {key} {value} {self.term}"
        # print(msg)
        write_to_file(self.log_path, msg2)

    if self.state == 0:
      self.reset_timer()

    if success:
      self.save_data()

    response = {
      'term': self.term,
      'success': success
    }
    return raft.AppendEntriesResponse(**response)
  
  def SetVal(self, request, context):
    key = request.key
    value = request.value
    if self.state == 2 and self.IsLeaseAcquired == True:
      entry = {
        'term': self.term,
        'update': ['set', key, value]
      }

      msg = f"Node {self.id} (leader) received a SET {key} {value} request."
      write_to_file(self.dump_path, msg)
      print(msg)

      self.log_table.append(entry)
      response = {
        'success': True,
        'leaderID': self.leader_id
      }

      return raft.SetValResponse(**response)
    else:
      response = {
        'success': False,
        'leaderID': self.leader_id
      }
      return raft.SetValResponse(**response)
  
  def GetVal(self, request, context):
    key = request.key

    if key in self.applied_entries and self.hasLeader == True:
      response = {
        'success': True,
        'value': self.applied_entries[key],
        'leaderID': self.leader_id
      }
      if (self.state == 2):
        msg = f"Node {self.id} (leader) received a GET {key} request."
        write_to_file(self.dump_path, msg)
        print(msg)
    else:
      response = {
        'success': False,
        'value': ' ',
        'leaderID': self.leader_id
      }
    return raft.GetValResponse(**response)

def run(handler: RaftServiceHandler):
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  raft_grpc.add_RaftServiceServicer_to_server(
      handler, server
  )
  server.add_insecure_port(f'[::]:{handler.address.port}')
  try:
    # print(f"Server has been started with address {handler.address}")
    server.start()
    server.wait_for_termination()
  except KeyboardInterrupt:
    exit()


def get_addr(id):
  with open('config.conf') as conf:
    while s := conf.readline():
      n_id, *n_address = s.split()
      if int(n_id) == id:
        address = Node_Address(int(n_id), n_address[0], int(n_address[1]))
  return address

if __name__ == "__main__":
  global MY_ADDR
  neighbours = []
  id = int(sys.argv[1])
  address = None
  with open('config.conf') as conf:
    while s := conf.readline():
      n_id, *n_address = s.split()
      if int(n_id) == id:
        address = Node_Address(int(n_id), n_address[0], int(n_address[1]))
        MY_ADDR = address

      n_ip = n_address[0]
      n_port = int(n_address[1])
      neighbours.append(Node_Address(int(n_id), n_ip, n_port))
  try:
    pass
    run(RaftServiceHandler(id, address, neighbours))
  except Exception as e:
    print(e)

MY_ADDR = None
