package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type Server struct {
	id            int
	state         ServerState
	currentTerm   int
	votedFor      int
	log           []string
	commitIndex   int
	lastApplied   int
	nextIndex     map[int]int
	matchIndex    map[int]int
	electionTime  time.Time
	heartbeatTime time.Time
	mu            sync.RWMutex
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	Entries  []string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type Cluster struct {
	servers map[int]*Server
	mu      sync.RWMutex
}

func NewCluster(numServers int) *Cluster {
	servers := make(map[int]*Server)
	for i := 0; i < numServers; i++ {
		servers[i] = &Server{
			id:           i,
			state:        Follower,
			currentTerm:  0,
			votedFor:     -1,
			log:          []string{},
			nextIndex:    make(map[int]int),
			matchIndex:   make(map[int]int),
			electionTime: time.Now().Add(time.Duration(150+rand.Intn(150)) * time.Millisecond),
		}
	}
	return &Cluster{servers: servers}
}

func (s *Server) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Term = s.currentTerm
	reply.VoteGranted = false

	if args.Term < s.currentTerm {
		return
	}

	if args.Term > s.currentTerm {
		s.currentTerm = args.Term
		s.votedFor = -1
		s.state = Follower
	}

	if (s.votedFor == -1 || s.votedFor == args.CandidateID) &&
		args.LastLogIndex >= len(s.log)-1 {
		s.votedFor = args.CandidateID
		reply.VoteGranted = true
		s.electionTime = time.Now().Add(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	}

	reply.Term = s.currentTerm
}

func (s *Server) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reply.Term = s.currentTerm
	reply.Success = false

	if args.Term < s.currentTerm {
		return
	}

	if args.Term > s.currentTerm {
		s.currentTerm = args.Term
		s.votedFor = -1
		s.state = Follower
	}

	if args.Term == s.currentTerm {
		s.state = Follower
		s.electionTime = time.Now().Add(time.Duration(150+rand.Intn(150)) * time.Millisecond)
		reply.Success = true
		s.log = append(s.log, args.Entries...)
	}

	reply.Term = s.currentTerm
}

func (s *Server) startElection(cluster *Cluster) {
	s.mu.Lock()
	s.currentTerm++
	s.state = Candidate
	s.votedFor = s.id
	term := s.currentTerm
	s.mu.Unlock()

	votes := 1

	for id, server := range cluster.servers {
		if id == s.id {
			continue
		}

		args := RequestVoteArgs{
			Term:        term,
			CandidateID: s.id,
		}
		reply := &RequestVoteReply{}

		server.RequestVote(args, reply)

		if reply.VoteGranted {
			votes++
		}

		if reply.Term > term {
			s.mu.Lock()
			s.currentTerm = reply.Term
			s.state = Follower
			s.votedFor = -1
			s.mu.Unlock()
			return
		}
	}

	majority := len(cluster.servers)/2 + 1
	if votes >= majority {
		s.becomeLeader(cluster)
	} else {
		s.mu.Lock()
		s.state = Follower
		s.mu.Unlock()
	}
}

func (s *Server) becomeLeader(cluster *Cluster) {
	s.mu.Lock()
	s.state = Leader
	s.mu.Unlock()

	for id := range cluster.servers {
		if id != s.id {
			s.nextIndex[id] = len(s.log)
			s.matchIndex[id] = 0
		}
	}
}

func (s *Server) sendHeartbeats(cluster *Cluster) {
	s.mu.RLock()
	if s.state != Leader {
		s.mu.RUnlock()
		return
	}
	term := s.currentTerm
	s.mu.RUnlock()

	for id, server := range cluster.servers {
		if id == s.id {
			continue
		}

		args := AppendEntriesArgs{
			Term:     term,
			LeaderID: s.id,
		}
		reply := &AppendEntriesReply{}

		server.AppendEntries(args, reply)

		if reply.Term > term {
			s.mu.Lock()
			s.currentTerm = reply.Term
			s.state = Follower
			s.mu.Unlock()
		}
	}
}

func (c *Cluster) runServer(id int) {
	server := c.servers[id]
	heartbeatTicker := time.NewTicker(50 * time.Millisecond)
	defer heartbeatTicker.Stop()

	for range heartbeatTicker.C {
		server.mu.RLock()
		isLeader := server.state == Leader
		electionTimeout := time.Until(server.electionTime)
		server.mu.RUnlock()

		if isLeader {
			server.sendHeartbeats(c)
		} else if electionTimeout <= 0 {
			fmt.Printf("Server %d starting election (term: %d)\n", id, server.currentTerm+1)
			server.startElection(c)
		}
	}
}

func (c *Cluster) printStatus() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stateNames := []string{"Follower", "Candidate", "Leader"}
	for id, server := range c.servers {
		server.mu.RLock()
		fmt.Printf("Server %d: %s, Term: %d, VotedFor: %d\n",
			id, stateNames[server.state], server.currentTerm, server.votedFor)
		server.mu.RUnlock()
	}
	fmt.Println()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cluster := NewCluster(3)

	for id := range cluster.servers {
		go cluster.runServer(id)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for i := 0; i < 10; i++ {
		<-ticker.C
		fmt.Printf("=== Status at %ds ===\n", i+1)
		cluster.printStatus()
	}
}
