import sys
import socket
import signal



httpheader = '''\
    HTTP/1.1 200 OK
    Context-Type: text/html
    Server: Python-slp version 1.0
    Context-Length: '''

class MyServer:
    def __init__(self, port, doc_root):
        self.port = port
        self.doc_root = doc_root
        self.host = "localhost"

    """
    
    Add your server and handlers here. 
      
    """
    """def HttpResponse(header,whtml):
        f = file(whtml)
        contxtlist = f.readlines()
        context = ''.join(contxtlist)
        response = "%s %d\n\n%s\n\n" % (header,len(context),context)
        return response
    """
    def analysedata():
        
    
    def runserver(self):
        print("host=",self.host,"port=",self.port)
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
            print("host=",self.host,"port=",self.port)
            s.bind((self.host, self.port))
            s.listen()
    
            conn,addre = s.accept()
            with conn:
                print ('connected by',addre)
                
                #这个while要套在外面！套在里面就会跑不出来！如下面注释部分 
                #因为recv函数在收到新的 data之前会一直处于等待状态。而break的条件是收到了data且是no data(我猜是crlf)
                while True:
                    data=conn.recv(1024)
                    if not data:
                        break
    #                while True:
    #                    data=conn.recv(1024)
    #                    if not data:
    #                        break
                    print (data)
                    a=anylysesdata(data)
                    #conn.send(MyServer.HttpResponse(httpheader,'index.html'))
                    conn.send(httpheader.encode())
                    
                conn.close()
            print ("done")
    




if __name__ == '__main__':
    input_port = int(sys.argv[1])
    input_doc_root = sys.argv[2]
    server = MyServer(input_port, input_doc_root)
    MyServer.runserver(server)        #use server as "slef" in class
    
    
    
#    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
#        
#        s.bind(("localhost", input_port))
#        s.listen()
#        print(1)
#
#        conn,addre = s.accept()
#        with conn:
#            print ('connected by',addre)
#            while True:
#                data=s.recv(1024)
#                if not data:
#                    break
#            print (data)
#            #conn.send(MyServer.HttpResponse(httpheader,'index.html'))
#            conn.send(httpheader)
#            conn.close()
#        print ("done")


    

    # Add code to start your server here
