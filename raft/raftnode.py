import rpyc
import sys
import threading
import time
import random
import os

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
		f=open(config)
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
		self.config_file = '/tmp/config-%d' % int(self.id)
		self.hasvoted=False
		self.hasvoted_new=False

		self.state='initial'
		self.currentTerm = 0


		try:
			self.load()
		except:
			pass

		


		# self.numVotes = 0
		# self.oldVotes = 0
		# self.newVotes = 0

		# self.lastLogIndex = 0
		# self.lastLogTerm = 0


		# self.during_change = 0
		# self.newPeers = []
		# self.new = None
		# self.old = None

		#self.last_update=time.time()
		

		#important!!!! need to start new threads and finish init, then the server can start and listen to other nodes
		if(self.state=='leader'):
			self.listener=threading.Thread(target=self.follower) #which should be self.leader()
			self.listener.start()
		else:
			self.listener=threading.Thread(target=self.follower)
			self.listener.start()




	def follower(self):
		if(self.state=='leader'): # cheat step when old lead re-start
			time.sleep(0.2)

		while(True):
			print ('Running as a follower')
			self.state = 'follower'

			self.save()
			
			self.last_update = time.time()
			self.election_timeout = 2 * random.random() + 1

			#'last_update'  changes in exposed functions. loop this until exposed functions are not used by other nodes during the timeout
			while (time.time() - self.last_update) <= self.election_timeout:
				pass
			self.start_election()

			while(self.state=='candidate'): #break this while loop and run follower again, if state change to 'follower' during candidate election (other node became leader and sent heartbeat to local node)
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
		self.last_update = time.time()


		self.state = 'candidate'
		print('became candidate')
		self.election_timeout = 2 * random.random() + 1
		
		self.currentTerm += 1

		self.save()

		# if two node block each other, need to timeout if nodes>=3
		#while(True): # time.time() - election_begintime < 4): # if time excceed election timeout(5s) became follower again
		
		# if not (self.state=='candidate'):
		# 	break
		
		self.hasvoted=True;
		self.votedFor = self.id
		self.voteTerm = self.currentTerm
		#self.save()
		self.numVotes = 1

		
		## print(self.ip_port[i])

		#multithread for requests
		self.election = [threading.Thread(target =self.thread_election, args = [i] )  for  i in range(len(self.ip_port)) ]
		for i in range(len(self.ip_port)):	
			self.election[i].start()
		for i in range(len(self.ip_port)):	
			self.election[i].join()#timeout=0.005)

		# for i in range(len(self.ip_port)):
		# 	port=self.ip_port[i].split(':')[1]
		# 	try:
		# 		print('try request to %d' % (int(port))  )
		# 		conn=rpyc.connect('localhost', int(port)).root
		# 		print(conn)
		# 	except:
		# 		pass
			#self.thread_election(i)
		if (self.numVotes>= (self.nodes_num/2.0) ):
			#self.leader()
			print('became leader')

			self.leader()
			#break
		else:
			print('only got %d vote(s) ' %(self.numVotes))
			#time.sleep(0.03 + 0.02 * random.random())



		while( (time.time() - self.last_update) <= self.election_timeout ):
			pass

		return
		
		
		# break from election timeout
		#self.follower()
		



	def thread_election(self,i):
		#print ('timouts, start a new election with term %d' % self.currentTerm)
		self.state = 'candidate'
		ip=self.ip_port[int(i)].split(':')[0]
		port=self.ip_port[int(i)].split(':')[1]
		#print(ip,port,type(ip))


		try:
			print('try request to %d' %(int(port)))
			conn=rpyc.connect(ip, int(port)).root
			#print(conn)

			
		except:
			return

		ans=conn.ans_request(self.id, self.currentTerm) #get vote from the peer node
		print(ans)
		self.numVotes+=ans
		return
		#conn.close()

		


	def leader(self):
		self.heartbeat_time = 0.2 * random.random() + 0.3
		self.state = 'leader'
		begin_time=time.time()


		self.save()


		while(True):
			if (self.state=='follower'):
				break
			self.heartbeat = [threading.Thread(target =self.thread_heartbeat, args = [i] )  for  i in range(len(self.ip_port)) ]
			for i in range(len(self.ip_port)):
				self.heartbeat[i].start()
			for i in range(len(self.ip_port)):	
				self.heartbeat[i].join()#timeout=0.005)
			
				
		

			if(time.time()-begin_time < self.heartbeat_time):
				time.sleep(begin_time+self.heartbeat_time-time.time())


		#break from state=='follower'
		self.follower()



	def thread_heartbeat(self,i):
		ip=self.ip_port[int(i)].split(':')[0]
		port=self.ip_port[int(i)].split(':')[1]

		try:
			print('heartbeat to %d' %(int(port)))
			conn=rpyc.connect(ip, int(port)).root
			# print(conn)
			
		except:
			return

		conn.rec_heartbeats(self.id, self.currentTerm)
		return










		## response from other nodes


	def exposed_ans_request(self, request_id, term):
		self.last_update = time.time()
		print('got request from %d' %(int(request_id)) )
		print(term, self.currentTerm)
		# if term>self.voteTerm:
		# 	self.hasvoted_new=False

		# if receive vote request from a new term
		# if (term > self.currentTerm): 
		# 	# self.state='follower'
		# 	if(self.hasvoted_new==False):  ###if a new new term call, need to vote also!!
		# 		self.votedFor_new=request_id
		# 		self.hasvoted_new=True
		# 		print('vote')
		# 		return 1
		# 	elif(self.votedFor_new==request_id):
		# 		print('vote')
		# 		return 1
		# 	else:
		# 		return 0
		# else: #if in same term, means already voted to itself, so return 0 directly
		# 	return 0


		if( term > self.currentTerm ):
			self.currentTerm=term #then can not vote for request in the same term
			self.votedFor = request_id
			self.hasvoted = True
			self.state='follower'
			return 1


		else: #current term as follower
			if(self.hasvoted==False):
				self.votedFor=request_id
				self.hasvoted=True
				return 1
			elif(self.votedFor==request_id):
				return 1
			else:
				return 0


	def exposed_rec_heartbeats(self, heartbeat_id, leaderTerm):
		if(leaderTerm>=self.currentTerm): #reject the heartbeat if its from an old leader
			print('got heartbeat from %d' %(int(heartbeat_id)))
			self.last_update = time.time()
			self.currentTerm = leaderTerm
			self.hasvoted_new = False # when reciving hearbeat, means a new term starts, and the next term vote reset
			#self.hasvoted = False
			self.state='follower'

	'''
		x = is_leader(): returns True or False, depending on whether
		this node is a leader

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call

		CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
	'''
	def exposed_is_leader(self):
		if(self.state=='leader'):
			return True
		else:
			return False



	def load(self):
		with open(self.config_file, 'r') as f:
			con = f.read()
			ll=con.split()
			self.state=ll[0]
			self.currentTerm[1]
		

	def save(self):
		#serverConfig = ServerConfig(self.poolsize, self.currentTerm, self.votedFor, self.log, self.peers)
		with open(self.config_file, 'w+') as f:
			f.write( str( self.state + " " + str(self.currentTerm)) )
			f.flush()
			os.fsync(f.fileno())




if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	print (sys.argv[3])
	port=int(sys.argv[3])

	
	server = ThreadPoolServer(RaftNode(sys.argv[1]), port=port)
	# print ('i', server)
	server.start()
	#server.follower()

