# Importing necessary libraries and modules
import sys
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc
import grpc
import threading
import time
import random
import os
from concurrent import futures
from raft_pb2 import Entry
import shutil


# Config file has servers addresses that'll be stored in servers.
CONFIG_PATH = "config.conf"  # overwrite this with your config file path
SERVERS = {}

# Server identifying variables.
TERM, VOTED, STATE, VOTES = 0, False, "Follower", 0
LEADER_ID = None
IN_ELECTIONS = False
SERVER_ID = int(sys.argv[1])
VOTED_NODE = -1
SERVER_ACTIVE = True
# Time limit in seconds for timeouts, and timer to set the time limit for certain functionalities.
MIN_ELECTION_TIMEOUT = 5
MAX_ELECTION_TIMEOUT = 10
TIME_LIMIT = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
LATEST_TIMER = -1

# Leader lease variables
LEADER_LEASE_INTERVAL = 4  # Leader lease interval in seconds (fixed value)
LEADER_LEASE_END_TIME = time.time() + LEADER_LEASE_INTERVAL  # Initialize to avoid immediate renewal
LATEST_LEADER_LEASE_TIMER = None  # Timer for renewing leader lease

# Threads used to request votes from servers as a candidate, and to append entries as a leader.
CANDIDATE_THREADS = []
HEARTBEAT_TIMER = None

# Commit Index is index of last log entry on server
# Last applied is index of last applied log entry on server
commitIndex, lastApplied, lastLogTerm = 0, 0, 0
# For applied commits.
ENTRIES = {}
# For non-applied commits.
LOGS = []

# NextIndex: list of indices of next log entry to send to server
# MatchIndex: list of indices of latest log entry known to be on every server
nextIndex, matchIndex = [], []
matchTerm = []
n_logs_replicated = 1

# Directory for logs and dumps
LOGS_DIR = f"logs_node_{SERVER_ID}"
LOG_FILE = os.path.join(LOGS_DIR, "log.txt")
DUMP_FILE = os.path.join(LOGS_DIR, "dump.txt")
METADATA_FILE = os.path.join(LOGS_DIR, "metadata.txt")

# Lock for file operations
FILE_LOCK = threading.Lock()


# Helper function to save messages in a file
def save_to_file(file_path, message):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_path, "a") as file:
        file.write(f"{message}\n")


# Leader lease mechanism
def renew_leader_lease():
    global LEADER_LEASE_END_TIME, LATEST_LEADER_LEASE_TIMER
    remaining_time = max(0, LEADER_LEASE_END_TIME - time.time())
    save_to_file(METADATA_FILE, f"Leader lease renewed. Remaining time: {remaining_time} seconds.")
    LEADER_LEASE_END_TIME = time.time() + LEADER_LEASE_INTERVAL
    if LATEST_LEADER_LEASE_TIMER is not None:
        LATEST_LEADER_LEASE_TIMER.cancel()  # Cancel previous timer if exists
    LATEST_LEADER_LEASE_TIMER = threading.Timer(LEADER_LEASE_INTERVAL, renew_leader_lease)
    LATEST_LEADER_LEASE_TIMER.start()


def reset_leader_timer():
    global LATEST_TIMER, TIME_LIMIT
    if isinstance(LATEST_TIMER, threading.Timer):
        LATEST_TIMER.cancel()
    TIME_LIMIT = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
    LATEST_TIMER = threading.Timer(TIME_LIMIT, leader_died)
    LATEST_TIMER.start()


# Heartbeat mechanism for leaders
def send_heartbeat():
    global LEADER_ID, SERVER_ID
    while True:
        time.sleep(1)
        if STATE == "Leader":
            replicate_logs()
            save_to_file(METADATA_FILE, "Heartbeat sent.")
            save_to_file(DUMP_FILE, f"Leader {LEADER_ID} sending heartbeat and renewing lease")
            # Renew leader lease upon sending heartbeat
            renew_leader_lease()

def replicate_log_to_followers(log_entry):
    global SERVERS, SERVER_ID

    for follower_id, follower_address in SERVERS.items():
        if SERVER_ID != follower_id:  # Exclude self
            try:
                channel = grpc.insecure_channel(follower_address)
                stub = pb2_grpc.RaftServiceStub(channel)
                request = pb2.SetValMessage(**log_entry)
                response = stub.SetVal(request)
                if not response.success:
                    save_to_file(METADATA_FILE, f"Failed to replicate log entry to follower {follower_id}")
            except grpc.RpcError:
                    save_to_file(METADATA_FILE, f"Failed to connect to follower {follower_id}")

# Client Interaction
# Updated RaftClient class
class RaftClient:
    def __init__(self, servers):
        self.servers = servers
        self.leader_id = None

    def get_leader_id(self):
        success, response = self.send_request("GetLeader")
        if success:
            self.leader_id = response.leaderId
        return success, response

    def set_leader(self, leader_id):
        self.leader_id = leader_id

    def send_request(self, request_type, key=None, value=None):
        if request_type == "GetLeader":
            try:
                channel = grpc.insecure_channel(self.servers[self.leader_id])
                stub = pb2_grpc.RaftServiceStub(channel)
                response = stub.GetLeader(pb2.GetLeaderResponse())
                return True, response
            except grpc.RpcError as e:
                return False, str(e)
        else:
            return False, "Invalid request type"

    def request_server(self, request_type, key=None, value=None):
        success, response = self.get_leader_id()
        if success:
            self.set_leader(response.leaderId)
            if response.leaderId == -1:
                return False, "No leader available"
            else:
                return self.send_request(request_type, key, value)
        else:
            return False, response


# Handler for RPC functions.
class RaftHandler(pb2_grpc.RaftServiceServicer):

    # This function is called by the Candidate during the elections to collect votes.
    def RequestVote(self, request, context):
        global TERM, VOTED, STATE, VOTED_NODE, IN_ELECTIONS, LATEST_TIMER
        global commitIndex, lastLogTerm
        candidate_term, candidate_id = request.term, request.candidateId
        candidateLastLogIndex, candidateLastLogTerm = request.lastLogIndex, request.lastLogTerm

        result = False
        IN_ELECTIONS = True
        if TERM < candidate_term:
            TERM = candidate_term
            result = True
            VOTED = True
            VOTED_NODE = candidate_id
            save_to_file(METADATA_FILE, f"Voted for node ==> {candidate_id}")
            run_follower()
        elif TERM == candidate_term:
            if VOTED or candidateLastLogIndex < len(LOGS) or STATE != "Follower":
                pass
            elif (candidateLastLogIndex == len(LOGS) and (LOGS[candidateLastLogIndex - 1]["TERM"] != candidateLastLogTerm)):
                pass
            else:
                result = True
                VOTED = True
                VOTED_NODE = candidate_id
                save_to_file(METADATA_FILE, f"Voted for node ==> {candidate_id}")
        reply = {"term": TERM, "result": result}
        return pb2.RequestVoteResponse(**reply)

    # This function is used by leader to append entries in followers.
    def AppendEntries(self, request, context):
        global TERM, STATE, LEADER_ID, VOTED, VOTED_NODE
        global LATEST_TIMER, commitIndex, ENTRIES, lastApplied

        leader_term, leader_id = request.term, request.leaderId
        reset_leader_timer()
        prevLogIndex, prevLogTerm = request.prevLogIndex, request.prevLogTerm
        entries, leaderCommit = request.entries, request.leaderCommit

        result = False
        if leader_term >= TERM:
            # Leader is already in a different term than mine.
            if leader_term > TERM:
                VOTED = False
                VOTED_NODE = -1
                TERM = leader_term
                LEADER_ID = leader_id
                run_follower()

            if prevLogIndex <= len(LOGS):
                result = True
                if len(entries) > 0:
                    LOGS.append({"TERM": leader_term, "ENTRY": entries[0]})
                    save_to_file(LOG_FILE, f"SET {entries[0]['key']}{entries[0]['value']} {TERM}")

                if leaderCommit > commitIndex:
                    commitIndex = min(leaderCommit, len(LOGS))
                    while commitIndex > lastApplied:
                        key, value = LOGS[lastApplied]["ENTRY"]["key"], LOGS[lastApplied]["ENTRY"]["value"]
                        ENTRIES[key] = value
                        lastApplied += 1

            # Renew leader lease upon receiving AppendEntries RPC
            renew_leader_lease()

        reply = {"term": TERM, "result": result}
        return pb2.AppendEntriesResponse(**reply)

    # This function is called from client to suspend server for PERIOD seconds.
    def Suspend(self, request, context):
        global SERVER_ACTIVE
        SUSPEND_PERIOD = int(request.period)
        save_to_file(LOG_FILE, f"Command from client: suspend {SUSPEND_PERIOD}")
        save_to_file(LOG_FILE, f"Sleeping for {SUSPEND_PERIOD} seconds")
        SERVER_ACTIVE = False
        time.sleep(SUSPEND_PERIOD)
        reset_timer(run_server_role, SUSPEND_PERIOD)
        return pb2.SuspendResponse(**{})

        
    def SetVal(self, request, context):
        key, val = request.key, request.value
        global ENTRIES, TERM, STATE, SERVER_ID

        try:
            if STATE == "Follower":
                # Redirect the request to the leader
                try:
                    channel = grpc.insecure_channel(SERVERS[LEADER_ID])
                    stub = pb2_grpc.RaftServiceStub(channel)
                    request = pb2.SetValMessage(**{"key": key, "value": val})
                    response = stub.SetVal(request)
                    return response
                except grpc.RpcError:
                    return pb2.SetValResponse(**{"success": False})
            elif STATE == "Candidate":
                return pb2.SetValResponse(**{"success": False})
            else:
                # Append the log entry to the leader's log file and replicate it to followers
                with FILE_LOCK:
                    with open(LOG_FILE, "a") as log_file:
                        log_entry = f"SET {key} {val} {TERM}"  # Format the log entry
                        log_file.write(log_entry + "\n")
                    LOGS.append({"TERM": TERM, "ENTRY": {"commandType": "set", "key": key, "value": val}})
                    ENTRIES[key] = val  # Update in-memory data
                    save_to_file(METADATA_FILE, f"Log entry appended to leader's log file: {log_entry}")
                    save_to_file(LOG_FILE, f"{log_entry} {TERM}")

                # Replicate the log entry to follower nodes
                replicate_log_to_followers({"key": key, "value": val})

                return pb2.SetValResponse(**{"success": True})
        except Exception as e:
            # Log any exceptions without crashing the node
            save_to_file(METADATA_FILE, f"Exception occurred while processing SetVal request: {str(e)}")
            return pb2.SetValResponse(**{"success": False})
    # This function is called from client to get value associated with key.
                    
    def GetVal(self, request, context):
        key = request.key
        global ENTRIES
        if key in ENTRIES:
            val = ENTRIES[key]
            return pb2.GetValResponse(**{"success": True, "value": val})
        return pb2.GetValResponse(**{"success": False})
    
    def NoOp(self, request, context):
        global TERM, LOG_FILE
        with open(LOG_FILE, "a") as log_file:
            log_file.write(f"NO-OP {TERM}\n")
        # Replicate the NO-OP entry to follower nodes
        replicate_no_op_to_followers()
        return pb2.NoOpResponse(success=True)


# Read config file to make the list of servers IDs and addresses.
def read_config(path):
    global SERVERS
    with open(path) as configFile:
        lines = configFile.readlines()
        for line in lines:
            parts = line.split()
            SERVERS[int(parts[0])] = (f"{str(parts[1])}:{str(parts[2])}")


# Runs the behaviour of changing from a follower:
# declares leader dead and becomes a candidate.
def leader_died():
    global STATE
    if STATE != "Follower":
        return
    save_to_file(METADATA_FILE, "Leader is dead")
    STATE = "Candidate"
    run_candidate()


def run_follower():
    global STATE
    STATE = "Follower"
    save_to_file(METADATA_FILE, f"I'm a follower. Term: {TERM}")
    reset_timer(leader_died, TIME_LIMIT)


def get_vote(candidate_id, server):
    global TERM, STATE, TIME_LIMIT, VOTES
    try:
        channel = grpc.insecure_channel(server)
        stub = pb2_grpc.RaftServiceStub(channel)
        request = pb2.RequestVoteMessage(**{"term": TERM, "candidateId": candidate_id,
                                            "lastLogIndex": len(LOGS) - 1, "lastLogTerm": lastLogTerm})
        response = stub.RequestVote(request)
        if response.term > TERM:
            TERM = response.term
            STATE = "Follower"
            TIME_LIMIT = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
            reset_timer(leader_died, TIME_LIMIT)
        if response.result:
            VOTES += 1
        return response.result
    except grpc.RpcError:
        pass


def process_votes():
    global STATE, LEADER_ID, CANDIDATE_THREADS, TIME_LIMIT, nextIndex, matchIndex
    for thread in CANDIDATE_THREADS:
        thread.join(0)
    save_to_file(METADATA_FILE, f"majority votes recieved")
    if VOTES > len(SERVERS) / 2:
        save_to_file(METADATA_FILE, f"I am a leader. Term: {TERM}")
        STATE = "Leader"
        LEADER_ID = SERVER_ID
        nextIndex = [len(LOGS) for i in range(len(SERVERS))]
        matchIndex = [0 for i in range(len(SERVERS))]
        run_leader()
    else:
        STATE = "Follower"
        TIME_LIMIT = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT)
        run_follower()


def run_candidate():
    global TERM, STATE, LEADER_ID, LATEST_TIMER, TIME_LIMIT, IN_ELECTIONS, VOTED_NODE
    global SERVER_ID, VOTES, CANDIDATE_THREADS, VOTED
    TERM += 1
    IN_ELECTIONS = True
    VOTED_NODE = -1
    CANDIDATE_THREADS = []
    VOTES = 1
    save_to_file(METADATA_FILE, f"I'm a candidate. Term: {TERM}.")
    # Requesting votes.
    for key, value in SERVERS.items():
        if SERVER_ID != key:  # Exclude self
            CANDIDATE_THREADS.append(threading.Thread(target=get_vote, args=(SERVER_ID, value)))
    # Check if you won the election and can become a leader.
    for thread in CANDIDATE_THREADS:
        thread.start()
    reset_timer(process_votes, TIME_LIMIT)

replicated_log_entries = set()


def replicate_log(key, server):
    global LOGS, STATE, matchIndex, matchTerm, nextIndex, n_logs_replicated, lastApplied, commitIndex, TERM, replicated_log_entries

    leaderCommit = commitIndex
    prevLogIndex = matchIndex[key]
    prevLogTerm = 0  # Default value, should be replaced with the actual term

    try:
        channel = grpc.insecure_channel(server)
        stub = pb2_grpc.RaftServiceStub(channel)
        
        # Create Entry messages from LOGS
        entries = []
        for log_entry_dict in LOGS:
            entry = pb2.Entry()
            entry.TERM = str(log_entry_dict.get('TERM', ''))
            entry.commandType = str(log_entry_dict.get('commandType', 'SET'))
            entry.key = str(log_entry_dict.get('key', ''))
            entry.value = str(log_entry_dict.get('value', ''))
            entries.append(entry)

        request = pb2.AppendEntriesMessage(
            term=TERM,
            leaderId=SERVER_ID,
            prevLogIndex=prevLogIndex,
            prevLogTerm=prevLogTerm,
            entries=entries,
            leaderCommit=leaderCommit,
            leaseInterval=LEADER_LEASE_INTERVAL
        )
        response = stub.AppendEntries(request)

        if response.term > TERM:
            STATE = "Follower"
            reset_timer(leader_died, TIME_LIMIT)

        if response.result:
            for log_entry in LOGS:
                if log_entry not in replicated_log_entries:
                    matchIndex[key] = nextIndex[key]
                    nextIndex[key] += 1
                    n_logs_replicated += 1
                    replicated_log_entries.add(log_entry)
        else:
            nextIndex[key] -= 1
            matchIndex[key] = min(matchIndex[key], nextIndex[key] - 1)
    except grpc.RpcError as rpc_error:
        print(f"Error occurred during RPC call: {rpc_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # If an unexpected error occurs, log the error and continue as leader
        pass  # Add any necessary recovery steps here

# Adjust the above function according to your actual implementation


# Adjust the above function according to your actual implementation

def run_leader():
    global TERM, LEADER_LEASE_INTERVAL, LEADER_ID
    # save_to_file(LOG_FILE, f"I'm a leader. Term: {TERM}")
    save_to_file(DUMP_FILE, f"Node {LEADER_ID} became the leader for term {TERM} ")

    def replicate_log_content():
        while True:
            leader_log_file_path = f"logs_node_{LEADER_ID}/log.txt"
            with open(leader_log_file_path, "r") as leader_log_file:
                leader_log_content = leader_log_file.read()

            # Iterate over each follower node
            for follower_id, follower_address in SERVERS.items():
                if LEADER_ID != follower_id:  # Exclude the leader node
                    follower_log_file_path = f"logs_node_{follower_id}/log.txt"
                    with open(follower_log_file_path, "w") as follower_log_file:
                        follower_log_file.write(leader_log_content)

            # Sleep for a short interval before reading the leader's log file again
            time.sleep(1)  
    log_replication_thread = threading.Thread(target=replicate_log_content)
    log_replication_thread.daemon = True
    log_replication_thread.start()


    renew_leader_lease()  # Start or renew the leader lease timer

    # # Append NO-OP entry to the leader's log file
    with open(LOG_FILE, "a") as log_file:
        log_file.write(f"NO-OP {TERM}\n")

    # # Replicate the NO-OP entry to follower nodes
    # replicate_no_op_to_followers()

    heartbeat_thread= threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon=True
    heartbeat_thread.start()
    while True:  # Loop indefinitely as leader
        if time.time() > LEADER_LEASE_END_TIME:  # Check if leader lease has expired
            save_to_file(DUMP_FILE, f"Leader {LEADER_ID} lease expired/renewl faild,  Stepping down...")
            # Step down as leader
            reset_leader_variables()  # Reset leader-related variables
            return

        # Send periodic heartbeats to all followers
        replicate_logs()


        # Sleep for half the lease interval
        time.sleep(LEADER_LEASE_INTERVAL / 2)


def replicate_logs():
    global LOGS, STATE, SERVERS, SERVER_ID

    if STATE != "Leader":
        return
    
    # Append log entries to the leader's log file
    # with FILE_LOCK:
    #     with open(LOG_FILE, "a") as log_file:
    #         for log_entry in LOGS:
    #             key = log_entry["ENTRY"]["key"]
    #             value = log_entry["ENTRY"]["value"]
    #             term = log_entry["TERM"]
    #             log_line = f"SET {key} {value} {term}"
    #             log_file.write(log_line + "\n")

    # Replicate each log entry to follower nodes
    for node_id, node_address in SERVERS.items():
        if SERVER_ID != node_id:  # Exclude self
            replicate_log(node_id, node_address)


def replicate_no_op_to_followers():
    global SERVERS, SERVER_ID, TERM

    # Iterate over follower nodes
    for follower_id, follower_address in SERVERS.items():
        if SERVER_ID != follower_id:  # Exclude self
            try:
                # Establish gRPC channel to follower node
                channel = grpc.insecure_channel(follower_address)
                stub = pb2_grpc.RaftServiceStub(channel)

                # Create request for NO-OP operation
                request = pb2.NoOpMessage()

                # Send NO-OP request to follower node
                response = stub.NoOp(request)

                if not response.success:
                    save_to_file(METADATA_FILE, f"Failed to replicate NO-OP entry to follower {follower_id}")
            except grpc.RpcError:
                save_to_file(METADATA_FILE, f"Failed to connect to follower {follower_id}")

def reset_timer(callback, interval):
    global LATEST_TIMER
    if isinstance(LATEST_TIMER, threading.Timer):
        LATEST_TIMER.cancel()
    LATEST_TIMER = threading.Timer(interval, callback)
    LATEST_TIMER.start()


def reset_leader_lease_timer():
    global LATEST_LEADER_LEASE_TIMER
    if LATEST_LEADER_LEASE_TIMER is not None:
        LATEST_LEADER_LEASE_TIMER.cancel()
        LATEST_LEADER_LEASE_TIMER = threading.Timer(LEADER_LEASE_INTERVAL, renew_leader_lease)
        LATEST_LEADER_LEASE_TIMER.start()


def reset_leader_variables():
    global LEADER_ID, nextIndex, matchIndex, LATEST_LEADER_LEASE_TIMER
    LEADER_ID = None
    nextIndex = []
    matchIndex = []
    if LATEST_LEADER_LEASE_TIMER is not None:
        LATEST_LEADER_LEASE_TIMER.cancel()
        LATEST_LEADER_LEASE_TIMER = None


def run_server_role():
    global STATE, SERVER_ACTIVE
    if not SERVER_ACTIVE:
        log("Server is suspended")
        return
    if STATE == "Leader":
        run_leader()
    elif STATE == "Candidate":
        run_candidate()
    elif STATE == "Follower":
        run_follower()
        # Reset leader-related variables if transitioning from leader to follower
        reset_leader_variables()
    else:
        log("Unknown state. Exiting...")
        sys.exit(1)


def log(message):
    print(message)


def main():
    global SERVERS, SERVER_ID, LOGS_DIR, LOG_FILE, DUMP_FILE
    if len(sys.argv) < 2:
        print("Please provide server ID as argument")
        sys.exit(1)

    SERVER_ID = int(sys.argv[1])
    read_config(CONFIG_PATH)
    LOGS_DIR = f"logs_node_{SERVER_ID}"
    LOG_FILE = os.path.join(LOGS_DIR, "log.txt")
    DUMP_FILE = os.path.join(LOGS_DIR, "dump.txt")
    METADATA_FILE= os.path.join(LOGS_DIR, "metadata.txt")

    initialize_files([LOG_FILE, DUMP_FILE, METADATA_FILE])

def initialize_files(files):
    for file_path in files:
        if not os.path.exists(file_path):
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(file_path, "w") as file:
                file.write("")
    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftServiceServicer_to_server(RaftHandler(), server)
    server.add_insecure_port(SERVERS[SERVER_ID])
    server.start()

    log("Server started...")
    try:
        while True:
            time.sleep(10)
            run_server_role()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    main()