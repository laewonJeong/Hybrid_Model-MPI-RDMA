#include "D_RoCELib.hpp"
#define port 40145
#define num_of_node 4
#define server_ip "10.10.10.101"

string node[num_of_node] = {server_ip,"10.10.10.102","10.10.10.103","10.10.10.104"};

char send_buffer[num_of_node][buf_size];
char recv_buffer[num_of_node][buf_size];

bool is_server(string ip){
  if(ip == server_ip)
    return true;
  return false;
}

int main(int argc, char* argv[]){
  if(argc != 2)
  {
    cerr << argv[0] << " <MY IP> " << endl;
    exit(1);
  }
  if(server_ip != node[0]){
    cerr << "node[0] is not server_ip" << endl;
    exit(1);
  }

  D_RoCELib d_rocelib;

  d_rocelib.initialize_connection(argv[1], node, num_of_node, port,send_buffer,recv_buffer);

  
  string ip = argv[1];
  string msg;
  
  cerr << "========================== many_to_many_communication ==========================\n" << endl;

  /* many to many communication*/
  msg = "[ " + ip + " ] Hi many-to-many communication!";
  
  d_rocelib.roce_comm(msg);
  for(int i=0;i<num_of_node-1;i++){
    printf("recv_buffer[%d]: %s\n", i, recv_buffer[i]); 
  }
  cerr << "\n========================== one_to_many_communication ==========================\n" << endl;

  /* 1 to many communication */
  if(is_server(ip)){
    msg = "[ " + ip + " ] Hi one-to-many communication!";
    d_rocelib.roce_one_to_many_send_msg(msg);
    cout << "ONE TO MANY SEND SUCCESS" << endl;
  }
  else{
    d_rocelib.roce_one_to_many_recv_msg();
    printf("recv_buffer[0]: %s\n", recv_buffer[0]);
  }

  cerr << "\n========================== many_to_one_communication ==========================\n" << endl;

  /* many to 1 communication */
  if(is_server(ip)){
    d_rocelib.roce_many_to_one_recv_msg();
    for(int i=0;i<num_of_node-1;i++){
      printf("recv_buffer[%d]: %s\n", i, recv_buffer[i]);
    }
  }
  else{
    msg = "[ " + ip + " ] Hi many-to-one communication!";
    d_rocelib.roce_many_to_one_send_msg(msg);

    cout << "MANY TO ONE SEND SUCCESS" << endl;
  }

  cerr << "================================================================================" << endl;

  //d_rdmalib.exit_rdma();
  
  return 0;
}
