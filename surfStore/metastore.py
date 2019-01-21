 #-*- coding: utf-8 -*-
import rpyc
import sys


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
	def __init__(self, message):
		super(ErrorResponse, self).__init__(message)
		self.error = message

	def missing_blocks(self, hashlist):
		self.error_type = 1
		self.missing_blocks = hashlist

	def wrong_version_error(self, version):
		self.error_type = 2
		self.current_version = version

	def file_not_found(self):
		self.error_type = 3



'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):
	

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config):
		f = open('%s' %(config))
		content = f.read()
		s = content.split()
		print (s)
		
		###和blockstore 建立rpyc连接
		portname = [i[10:] for i in s if (i.startswith('localhost:') & (not i.startswith('localhost:6000')) )]
		self.conn_blockstore = [rpyc.connect('localhost', int(d)) for d in portname] 
		print (self.conn_blockstore)

		#metadata的本地变量，有两个，分别为两个hash table. {filename:hashkey} & {filename:version}
		self.hl_byfilename = {}
		self.file_version = {}
		self.hl_byfilename_temp = {}



	'''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_modify_file(self, filename, version, hashlist):
		
		self.hl_byfilename_temp[filename]=[]
		missing_blocks_id = []

		#检查文件版本
		#初始化
		if not filename in self.file_version:
			self.file_version[filename] = 0
		#检查版本
		if (version == self.file_version[filename]+1 ):
			print ("version match, ready for upload")
		else:
			response = ErrorResponse('wrong version number')
			response.wrong_version_error(version)
			raise response
			#return [-1, self.file_version[filename]+1] 



		'''检查blockstore，得到missing blocks'''
		for i in range(len(hashlist)):
			#更新 hl_byfilename 临时表
			self.hl_byfilename_temp[filename].append(hashlist[i])

			a = self.findServer(hashlist[i])
			exist = self.conn_blockstore[a].root.has_block(hashlist[i])
			if not exist:
				missing_blocks_id.append(i)


		#全部upload成功了 version再+1,且更新真正的hashlist
		if len(missing_blocks_id) == 0:
			print ('refresh version & hl')
			self.file_version[filename] += 1
			self.hl_byfilename[filename] = self.hl_byfilename_temp[filename]
		return missing_blocks_id



	'''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_delete_file(self, filename, version):
		if not self.hl_byfilename[filename]==[]:
			#self.file_version[filename] = 0
			
			#检查版本
			if (version == self.file_version[filename]+1 ):
				print ("version match, ready for delete")
				self.hl_byfilename[filename]=[]
				self.file_version[filename]+=1
			else:
				response = ErrorResponse('wrong version number')
				response.wrong_version_error(version)
				raise response
				#return [-1, self.file_version[filename]+1]
			

		else:
			response = ErrorResponse('Not Found')
			response.file_not_found()
			raise response
		




	'''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
	def exposed_read_file(self, filename):
		if filename in self.file_version:
			v = self.file_version[filename]
			hl = self.hl_byfilename[filename]
		else:
			v = 0
			hl = []

		return (v,hl)



	def findServer(self,h):
		return int(h,16) % len(self.conn_blockstore)


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
	server.start()

