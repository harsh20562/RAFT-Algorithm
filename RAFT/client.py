import grpc
import raft_pb2_grpc
import raft_pb2
from helper import Address

class Client:
  def __init__(self) -> None:
    self.address = None
    self.channel = None
    self.stub = None

  def connect(self, ip: str, port: int) -> None:
    self.address = Address(ip, port)
    self.channel = grpc.insecure_channel(f"{self.address.ip}:{self.address.port}")
    self.stub = raft_pb2_grpc.RaftServiceStub(self.channel)
  
  def set_value(self, key, value):
    if not self.address:
      raise KeyError("Address not set")
    request = {'key': key, 'value': value}
    try:
      response = self.stub.SetVal(raft_pb2.SetValRequest(**request))
      if not response.success:
        print(f"Failure | leaderID: {response.leaderID}")
        return False
      else:
        print("Success")
        return True
    except grpc.RpcError as e:
      print(e)
      return False
  
  def get_value(self, key):
    if not self.address:
      raise KeyError("Address not set")
    request = {'key': key}
    try:
      response = self.stub.GetVal(raft_pb2.GetValRequest(**request))
      return f"{response.value}"
    except grpc.RpcError as e:
      print(e)
      return False
  
if __name__ == "__main__":
  client = Client()
  while True:
    print(f"1. Connect to Node\n2. Set Value\n3. Get Value\n4. Exit")
    choice = int(input())
    if choice == 1:
      ip = input("Enter IP: ")
      port = int(input("Enter Port: "))
      client.connect(ip, port)
    elif choice == 2:
      key = input("Enter Key: ")
      value = input("Enter Value: ")
      client.set_value(key, value)
    elif choice == 3:
      key = input("Enter Key: ")
      print(client.get_value(key))
    elif choice == 4:
      break
    else:
      print("Invalid Choice")