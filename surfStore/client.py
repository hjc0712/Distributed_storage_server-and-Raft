 #-*- coding: utf-8 -*-
import rpyc
import hashlib
import os
import sys

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""

class SurfStoreClient():

	"""
	Initialize the client and set up connections to the block stores and
	metadata store using the config file
	"""
	def __init__(self, config):

		#和metadata 建立rpyc连接
		self.conn = rpyc.connect('localhost', 6000).root

		#和blockstore 建立rpyc连接
		f = open('%s' %(config))
		content = f.read()
		s = content.split()
		#print (s)


		portname = [i[10:] for i in s if (i.startswith('localhost:') & (not i.startswith('localhost:6000')) )]
		self.conn_blockstore = [rpyc.connect('localhost', int(d)) for d in portname] 
		#print (self.conn_blockstore)

		#检验本地存在哪些块（通过hl值验证）
		self.block_byhashname = {} #client的block存储，{hashname: blockdata}
		

	"""
	upload(filepath) : Reads the local file, creates a set of 
	hashed blocks and uploads them onto the MetadataStore 
	(and potentially the BlockStore if they were not already present there).
	"""
	def upload(self, filepath):
		
		#读入文件
		try:
			f=open('%s' %(filepath),'rb')
		except IOError:
			print ('Not Found')
			return
		#把file 分成 blocks
		blocks=[]
		while True:
			bb=f.read(4096)
			if(len(bb)==0):
				break
			blocks.append(bb)
			
			f.seek(4096,1)
			
			
		
		#计算各个blocks的hash
		hl = []
		for i in range(len(blocks)):
			hl.append(hashlib.sha256(blocks[i]).hexdigest())
		#get filename form filepath
		filename = filepath.split('/')[-1]



		#检查是否已经有旧的版本
		#调用metadata中的readfile函数，返回是否已经有旧的版本。如果有，返回版本号和已有的hl值
		version,hl_old = self.conn.read_file(filename)

		#如果需要更新，执行更新	
		version+=1
		
		#循环，直到所有的missingblock都被写入（防止写入时候出错,同时解决concurency时候verision的问题）
		while True:
			
			#调用metadat中的modify函数，返回一个（需要上传的）hash值的list
			try:
				#print ('modify once')
				missing_block_id = self.conn.modify_file(filename,version,hl)
			
			except Exception as error:
				#concurency时候wrong version number, then version+1 and do the while loop again
				if(error.error_type==2):
					print ('Error: Required version >=%d'%(missing_block_id[1]))
					version = error.current_version+1

			#在此检查是否已经全部写入missing block
			if len(missing_block_id)==0:
				break

			#和 blockstore进行通讯， 传文件给blockstore
			for i in missing_block_id:
				a=self.findServer(hl[i])
				self.conn_blockstore[a].root.store_block(hl[i],blocks[i])

		print ('OK')

		version,hl = self.conn.read_file(filename)
		#print(type(hl))
		#print (version, hl)


		


	"""
	delete(filename) : Signals the MetadataStore to delete a file.
	"""
	def delete(self, filename):
		
		version,hl=self.conn.read_file(filename)

		while True:
			try:
				self.conn.delete_file(filename,version+1)
				print ('OK')
				break
			except Exception as error:
				if error.error_type==2:
					#print ('Error: Required version >=%d'%(missing_block_id[1]))
					version=error.current_version+1
				elif error.error_type==3:
					print (error.error)
					break

		




	"""
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
	"""
	def download(self, filename, location):
		#get version & hl from metadata
		version,hl=self.conn.read_file(filename)
		
		if (len(hl)==0):
			print('Not Found')
			return

		#get missing blocks form blockstore
		
		for hashname in hl:
			if not hashname in self.block_byhashname: #如果missing,（在client的存储中没有当前block）
				#去blockstore里面取
				a=self.findServer(hashname)
				bb=self.conn_blockstore[a].root.get_block(hashname)
				
				self.block_byhashname[hashname] = bb #更新missing状态

		#merge & save to the right location
		blocks=[]
		for hashname in hl:
			blocks.append(self.block_byhashname[hashname])
		file_content = b''.join(blocks)

		f=open('%s/%s'%(location,filename),'wb')
		f.write(file_content)
		f.close()
		print ('OK')

		
		

	"""
	 Use eprint to print debug messages to stderr
	 E.g - 
	 self.eprint("This is a debug message")
	"""
	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)


	def findServer(self,h):
		return int(h,16) % len(self.conn_blockstore)


if __name__ == '__main__':
	client = SurfStoreClient(sys.argv[1])
	operation = sys.argv[2]
	if operation == 'upload':
		client.upload(sys.argv[3])
	elif operation == 'download':
		client.download(sys.argv[3], sys.argv[4])
	elif operation == 'delete':
		client.delete(sys.argv[3])
	else:
		print("Invalid operation")
		
