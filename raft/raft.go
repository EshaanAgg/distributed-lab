package raft

// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"

	"github.com/eshaanagg/distributed/labrpc"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const MinimumElectionTimeout = 300 * time.Millisecond
const MaximumElectionTimeout = 400 * time.Millisecond
const HeartbeatTimeout = 50 * time.Millisecond

func getElectionTimeout() time.Duration {
	return MinimumElectionTimeout + time.Duration(rand.Int63n(int64(MaximumElectionTimeout-MinimumElectionTimeout)))
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	nextElectionTimeout time.Time // The time at which the next election timeout will occur

	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    *int
	log         []string

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	role       Role
	nextIndex  []int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// Return currentTerm and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term         int // Candidate’s term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// RequestVote RPC reply structure
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // Bool indicating if the candidate received vote
}

// RequestVote RPC handler
// Grant vote if
//   - Candidate's term is at least as up-to-date as receiver's term
//   - Have not voted for another candidate
//   - Candidate's log is at least as up-to-date as receiver's log
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO: B
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.mu.Lock()
		rf.votedFor = &args.CandidateId
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int // Leader’s term
	LeaderId     int
	prevLogIndex int // Index of log entry immediately preceding new ones
	prevLogTerm  int // Term of prevLogIndex entry
	// entries[] log entries to store (empty for heartbeat; may send more than one for efficiency)
	leaderCommit int // Leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // Bool indicating if the follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return
	}

	// Become a follower since the leader is known and update the timeout
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.nextElectionTimeout = time.Now().Add(getElectionTimeout())
}

func (rf *Raft) conductElection() {
	rf.mu.Lock()

	if rf.role != Follower || time.Now().Before(rf.nextElectionTimeout) {
		rf.mu.Unlock()
		return
	}
	DPrintf("[%d] is conducting election", rf.me)

	// Transition to candidate state
	rf.currentTerm++
	rf.votedFor = &rf.me
	rf.role = Candidate
	rf.nextElectionTimeout = time.Now().Add(getElectionTimeout())
	rf.mu.Unlock()

	// Initialize the voting procedure
	var voteMutex sync.Mutex
	voteCount := 1
	totalCount := 1
	cond := sync.NewCond(&voteMutex)

	for ind, peer := range rf.peers {
		if ind != rf.me {
			go func(ind int, peer *labrpc.ClientEnd) {
				reply := RequestVoteReply{}
				ok := peer.Call("Raft.RequestVote", &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}, &reply)

				if !ok {
					DPrintf("[%d] failed to contact [%d] via Raft.RequestVote RPC", rf.me, ind)
				} else {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
				}

				voteMutex.Lock()
				defer voteMutex.Unlock()
				if ok && reply.VoteGranted {
					voteCount++
				}
				totalCount++
				cond.Broadcast()
			}(ind, peer)
		}
	}

	voteMutex.Lock()
	defer voteMutex.Unlock()

	for 2*voteCount <= len(rf.peers) && totalCount != len(rf.peers) {
		cond.Wait()
	}

	if 2*voteCount > len(rf.peers) {
		rf.mu.Lock()
		// This condition is to ensure that there has been no other leader elected since the election began
		if rf.role == Candidate {
			DPrintf("[%d] becomes the leader: Recieved %d/%d", rf.me, voteCount, totalCount)
			rf.role = Leader
			rf.mu.Unlock()

			// Send a AppendEntries RPC to all other servers to establish leadership
			for ind, peer := range rf.peers {
				if ind != rf.me {
					go func(ind int, peer *labrpc.ClientEnd) {
						reply := AppendEntriesReply{}
						ok := peer.Call("Raft.AppendEntries", &AppendEntriesArgs{
							Term:     rf.currentTerm,
							LeaderId: rf.me,
						}, &reply)
						if !ok {
							DPrintf("[%d] failed to contact [%d] via Raft.AppendEntries RPC", rf.me, ind)
						}
					}(ind, peer)
				}
			}
		}
	} else {
		DPrintf("[%d] lost the election: Recieved %d/%d", rf.me, voteCount, totalCount)
		rf.role = Follower
	}
}

// Kick a goroutine that periodically checks if an election needs to be conducted
func (rf *Raft) checkElection() {
	for {
		if rf.role == Follower && time.Now().After(rf.nextElectionTimeout) {
			rf.conductElection()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	// Your code here (2B).

	return index, rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Kill() {
	DPrintf("[%d] is killed", rf.me)
}

// Kick a goroutine that periodically sents heartbeats to all other servers
// if this server is the leader
func (rf *Raft) sendHeartbeats() {
	for {
		if rf.role == Leader {
			for ind, peer := range rf.peers {
				if ind != rf.me {
					go func(ind int, peer *labrpc.ClientEnd) {
						args := AppendEntriesArgs{
							Term:     rf.currentTerm,
							LeaderId: rf.me,
						}
						reply := AppendEntriesReply{}
						ok := peer.Call("Raft.AppendEntries", &args, &reply)
						if !ok {
							DPrintf("[%d] failed to contact [%d] via Raft.AppendEntries RPC", rf.me, ind)
						}
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
					}(ind, peer)
				}
			}
		}
		time.Sleep(HeartbeatTimeout)
	}
}

// The main loop for the Raft server
func (rf *Raft) run() {
	// TODO: A,B,C
	go rf.sendHeartbeats()
	go rf.checkElection()
}

// Code used by the service or tester to create a Raft server.
// The ports of all the Raft servers (including this one) are in peers[].
// This server's port is peers[me].
// All the servers' peers[] arrays have the same order.
// Persister is a place for this server to save its persistent state, and also initially holds the most recent saved state, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nextElectionTimeout = time.Now().Add(getElectionTimeout())

	rf.run()

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
