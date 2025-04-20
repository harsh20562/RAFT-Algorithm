import os

class Address:
  def __init__(self, ip: str, port: int) -> None:
    self.ip = ip
    self.port = port

class Node_Address:
  def __init__(self, id: int, ip: str, port: int):
    self.id = id
    self.ip = ip
    self.port = port

def create_dir(dir_name: str) -> None:
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)

def create_file(file_name: str) -> None:
  if not os.path.exists(file_name):
    open(file_name, 'w').close()

def write_to_file(file_path, line):
  with open(file_path, 'a') as file:
    file.write(line + '\n')