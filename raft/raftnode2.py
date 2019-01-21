import rpyc
import sys
import threading
import time
import random

'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''
class RaftNode(rpyc.Service):
	

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config):
		nodeId=sys.argv[2]
		print (nodeId)
		f=open('config.txt')
		con=f.readline()
		self.nodes_num= int( con.split(' ')[1] )
		self.nodes=[]
		self.ip_port=[]
		for i in range(self.nodes_num):
			con=f.readline()
			ll=con.split(' ')
			if not i == int(nodeId):
				self.nodes.append(ll[0])
				self.ip_port.append(ll[1][:-1])

		#print(con)
		print(self.ip_port)

		self.id=nodeId
		self.hasvoted=False;

		self.state='initial'
		self.currentTerm = 0


		self.numVotes = 0
		self.oldVotes = 0
		self.newVotes = 0

		self.lastLogIndex = 0
		self.lastLogTerm = 0


		self.during_change = 0
		self.newPeers = []
		self.new = None
		self.old = None
		
		

		self.follower();
		#pass


	def follower(self):
		print ('Running as a follower')
		self.state = 'follower'
		self.last_update = time.time()
		election_timeout = 1.5 * random.random() + 5

		#'last_update'  changes in exposed functions. loop this until exposed functions are not used by other nodes during the timeout
		while time.time() - self.last_update <= election_timeout:
			pass
		self.start_election()
		# while True:
		# 	self.last_update = time.time()
		# 	election_timeout = 5 * random.random() + 5
		# 	while time.time() - self.last_update <= election_timeout:
		# 		pass

		# 	if self.election.is_alive():
		# 		self.election.kill()
		# 	self.start_election()

	def start_election(self):
		self.state = 'candidate'
		print('became candidate')
		self.currentTerm += 1
		election_begintime=time.time()

		count=0
		while(True):
			count+=1
			self.hasvoted=False;
			self.votedFor = self.id
			#self.save()
			self.numVotes = 1

			
			## print(self.ip_port[i])
			# self.election = [threading.Thread(target =self.thread_election, args = [i] )  for  i in range(len(self.ip_port)) ]
			# for i in range(len(self.ip_port)):	
			# 	self.election[i].start()
			# for i in range(len(self.ip_port)):	
			# 	self.election[i].join()

			for i in range(len(self.ip_port)):
				self.thread_election(i)



			if (self.numVotes>= (self.nodes_num/2.0) ):
				#self.leader()
				print('became leader')
				break
			else:
				print('only got %d vote(s) in %d' %(self.numVotes,count))
				time.sleep(0.05)

		#self.leader()

			# if len(self.peers) != 0:
			# 	self.currentTerm += 1
			# 	self.votedFor = self.id
			# 	self.save()
			# 	self.numVotes = 1
			# 	if self.during_change == 1:
			# 		self.newVotes = 0
			# 		self.oldVotes = 0
			# 		if self.id in self.new:
			# 			self.newVotes = 1
			# 		if self.id in self.old:
			# 			self.oldVotes = 1
			# 	elif self.during_change == 2:
			# 		self.newVotes = 0
			# 		if self.id in self.new:
			# 			self.newVotes = 1
			# 	self.election.start()


	def thread_election(self,i):
		#print ('timouts, start a new election with term %d' % self.currentTerm)
		self.state = 'candidate'
		ip=self.ip_port[int(i)].split(':')[0]
		port=self.ip_port[int(i)].split(':')[1]
		#print(ip,port,type(ip))
		aaa='7000'
		conn=rpyc.connect('localhost', int(aaa)).root
		print(conn)

		try:
			print('try request to %d' %(int(port)))
			conn=rpyc.connect('localhost', int(port)).root
			print(conn)
			
		except:
			return

		ans=conn.ans_request(self.id) #get vote from the peer node
		print(ans)
		self.numVotes+=ans
		conn.close()

		

		#request for vites
		


		# self.request_votes = self.peers[:]
		# sender = self.id

		# while 1:
		# 	# print 'Send vote request to ', self.request_votes
		# 	for peer in self.peers:
	 # 			if peer in self.request_votes:
	 # 				Msg = str(self.lastLogTerm) + ' ' + str(self.lastLogIndex)
	 # 				msg = RequestVoteMsg(sender, peer, self.currentTerm, Msg)
	 # 				data = pickle.dumps(msg)
	 # 				sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	 # 				sock.sendto(data, ("", self.addressbook[peer]))
 	# 		time.sleep(1) # wait for servers to receive





	def exposed_ans_request(self, request_id):
		self.last_update = time.time()
		print('got request from %d' %(request_id))
		if(self.hasvoted==False):
			self.votedFor=request_id
			self.hasvoted==True
			return 1
		else:
			return 0

	def exposed_rec_heartbeats(self, heartbeat_id):
		self.last_update = time.time()

	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
	'''
	def exposed_is_leader(self):
		return False


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	print (sys.argv[3])
	port=int(sys.argv[3])
	server = ThreadPoolServer(RaftNode(sys.argv[1]), port=port )
	# print ('i', server)
	server.start()

