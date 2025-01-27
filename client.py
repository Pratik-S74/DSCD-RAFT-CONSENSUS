import grpc
import raft_pb2
import raft_pb2_grpc

class RaftClient:
    def __init__(self, server_address):
        self.server_address = server_address
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = raft_pb2_grpc.RaftServiceStub(self.channel)

    def get_leader(self):
        try:
            response = self.stub.GetLeader(raft_pb2.GetLeaderMessage())
            return response.leaderId, response.leaderAddress
        except grpc.RpcError as e:
            print("Error:", e)
            return -1, ""

    def suspend_server(self, period):
        try:
            response = self.stub.Suspend(raft_pb2.SuspendMessage(period=period))
            print("Server suspended for", period, "seconds.")
        except grpc.RpcError as e:
            print("Error:", e)

    def get_value(self, key):
        try:
            response = self.stub.GetVal(raft_pb2.GetValMessage(key=key))
            if response.success:
                print("Value for key", key, "is:", response.value)
            else:
                print("Key", key, "not found.")
        except grpc.RpcError as e:
            print("Error:", e)

    def set_value(self, key, value):
        try:
            response = self.stub.SetVal(raft_pb2.SetValMessage(key=key, value=value))
            if response.success:
                print("Value", value, "successfully set for key", key)
            else:
                print("Failed to set value for key", key)
        except grpc.RpcError as e:
            print("Error:", e)

    def close_channel(self):
        self.channel.close()

def main():
    server_address = input("Enter server address (host:port): ")
    client = RaftClient(server_address)

    while True:
        print("\n1. Set Value\n2. Get Value\n3. Exit\n")
        choice = input("Enter your choice: ")

        if choice == '1':
            key = input("Enter key: ")
            value = input("Enter value: ")
            client.set_value(key, value)

        elif choice == '2':
            key = input("Enter key to get value: ")
            client.get_value(key)
        
        elif choice == '3':
            client.close_channel()
            print("Exiting...")
            break

        
        # elif choice == '4':

        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

    