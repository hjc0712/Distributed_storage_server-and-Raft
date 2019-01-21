import sys
import socket
import threading
import time





class MyServer:
    def __init__(self, port, doc_root):
        self.port = port
        self.doc_root = doc_root
        self.host = "localhost"

    """
    
    Add your server and handlers here. 
      
    """
    
    def hasdir(self, path):
        if(path=='/'):
            path='/index.html'
        try: 
            open('%s%s' %(self.doc_root, path))
        except IOError:
            return False
        
        #if flee root
        if( ('/../..' in path) | (path.startswith('/..')) ):
            return False
        
        return True
    
    
    def analysedata(self, data):
        s=data.split()
        print ('data_split=',s)
        
        if(len(s)<5):
            return '400'
        #200
        elif( (s[0]=='GET') & self.hasdir(s[1]) & ('HTTP'in s[2])  & (s[3]=='Host:') ):
            if( ('  ' not in data) & ('User:Agent' not in data) ):
                return '200'
            else:
                return '400'
        #404
        elif( (s[0]=='GET')  & ('HTTP'in s[2]) & (s[3]=='Host:') ):
            return '404'
        #400
        else:
            return '400'
    
    
#    def gethtml(self, path):
#        if(path=='/'):
#            path='/index.html'
#        f=open('%s%s'%(self.doc_root,path))
#        contentlist=f.readlines()
#        content=''.join(contentlist)
#        httpheader200 = 'HTTP/1.1 200 OK\r\nServer: Python-slp version 1.0\r\nLast-Modified: Sun, 19 Aug 18 18:02:49 -0700\r\nContent-Type: text/html\r\nContent-Length: '
#        response='%s%d\r\n\r\n%s'%(httpheader200,len(content),content)
#        return response
        
    def get200file(self, path):
        if(path=='/'):
            path='/index.html'
        f=open('%s%s'%(self.doc_root,path), 'rb')
        content=f.read()
        httpheader200 = 'HTTP/1.1 200 OK\r\nServer: Python-slp version 1.0\r\nLast-Modified: Sun, 19 Aug 18 18:02:49 -0700\r\nContent-Type: text/html\r\nContent-Length: '
        response=('%s%d\r\n\r\n'%(httpheader200,len(content))).encode() + content
        return response
    
    
    
    
    
    def threadserver(self, conn, addr):
        with conn:
            print ('connected by',addr)
            
            
            
            #receive data----------------------------
            
            
                
            #a single recv
            #if get \r\n\r\n to break,   if not, waituntil timeout and return 400
            data=''
            while True:
                try:
                    if '\r\n\r\n' in data:  
                        break
            
                        
                    else:
                        
                        dd= (conn.recv(1024)).decode('utf-8') #byte when transport, string when handle
                        data += dd
                        conn.settimeout(5)
                    
                except socket.timeout:
                    if(data!=''):
                        conn.send('HTTP/1.1 400 Client Error\r\nServer: Python-slp version 1.0\r\n\r\n'.encode())
                    conn.close()
#                        timeout=True
#                        print ('timeout',timeout)
#                if '\r\n\r\n' in data:  
#                        break
#                else:
#                    dd= (conn.recv(1024)).decode('utf-8') #byte when transport, string when handle
#                    data += dd
#                
#                if data!='':
#                    conn.settimeout(5)
            
            
            
            print (data)
            
            
            
            #validate data----------------------------
            a=self.analysedata(data)
            print (a)
            
            
            #send response----------------------------
            
            if(a=='200'):
                #html
                if((data.split()[1]).endswith('.html') | (data.split()[1]=='/' )) :
                    data200=self.get200file( (data.split()[1]) )
                
                #jpg
                else:
                    data200=self.get200file( (data.split()[1]) )
                
                
                responsedata=data200
                
            elif(a=='400'):
                responsedata=('HTTP/1.1 400 Client Error\r\nServer: Python-slp version 1.0\r\n\r\n').encode()
            else:
                responsedata=('HTTP/1.1 404 Not found\r\nServer: Python-slp version 1.0\r\n\r\n').encode() 
                
            conn.send(responsedata)
            
            
            # close connection or continue
            if( ('Connection: close' in data) ):
                conn.close()
            else:
                time.sleep(5)
                    
                    
            conn.close()
            print ("done")
    
    
    
    
    def runserver(self):
        print("host=",self.host,"port=",self.port)
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
            print("host=",self.host,"port=",self.port)
            s.bind((self.host, self.port))
            s.listen(10)
            
    
            while True:
                conn,addr = s.accept()
                thread = threading.Thread(target=self.threadserver, args=(conn, addr))
                thread.start()
                
                
    




if __name__ == '__main__':
    input_port = int(sys.argv[1])
    input_doc_root = sys.argv[2]
    server = MyServer(input_port, input_doc_root)
    
    #MyServer.runserver(server)        #use server as "slef" in class\
    server.runserver()
    
    
    
    


    

    # Add code to start your server here
