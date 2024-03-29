#include "tcp.hpp"
#include "RDMA.hpp"
#include "D_RoCELib.hpp"

static std::mutex mutx;
D_RoCELib myrdma;

char* change(string temp){
  static char stc[buf_size];
  strcpy(stc, temp.c_str());
  return stc;
}
void D_RoCELib::set_buffer(char send[][buf_size], char recv[][buf_size], int num_of_server){
    myrdma.send_buffer = &send[0];
    myrdma.recv_buffer = &recv[0];
    myrdma.connect_num = num_of_server - 1;
}
void D_RoCELib::rdma_send(string msg, int i){
    RDMA rdma;
    if (msg.size() > 67108863)
        msg.replace(67108864,67108864, "\0");
    //msg[67108865] = NULL;
    strcpy(myrdma.send_buffer[i],msg.c_str());
    
    rdma.post_rdma_send(get<4>(myrdma.rdma_info[0][i]), get<5>(myrdma.rdma_info[0][i]), myrdma.send_buffer[i], 
                                sizeof(myrdma.send_buffer[i]), myrdma.qp_key[i].first, myrdma.qp_key[i].second);
    if(!rdma.pollCompletion(get<3>(myrdma.rdma_info[0][i])))
        //cerr << "send success" << endl;
        cerr << "send failed" << endl;
    
}

void D_RoCELib::rdma_send_with_imm(string msg, int i){
    RDMA rdma;
    
    if (msg.size() > 67108863)
        msg.replace(67108864,67108864, "\0");
    //msg[67108865] = NULL;
    strcpy(myrdma.send_buffer[i],msg.c_str());
    
    rdma.post_rdma_send_with_imm(get<4>(myrdma.rdma_info[0][i]), get<5>(myrdma.rdma_info[0][i]), myrdma.send_buffer[i], 
                                sizeof(myrdma.send_buffer[i]), myrdma.qp_key[i].first, myrdma.qp_key[i].second);
    if(!rdma.pollCompletion(get<3>(myrdma.rdma_info[0][i])))
        //cerr << "send success" << endl;
        cerr << "send failed" << endl;
}
void D_RoCELib::rdma_write(string msg, int i){
    RDMA rdma;
    TCP tcp;
    if (msg.size() > 67108863)
        msg.replace(67108864,67108864, "\0");
    //msg[67108865] = NULL;
    strcpy(myrdma.send_buffer[i],msg.c_str());
    
    rdma.post_rdma_write(get<4>(myrdma.rdma_info[0][i]), get<5>(myrdma.rdma_info[0][i]), myrdma.send_buffer[i], 
                         sizeof(myrdma.send_buffer[i]), myrdma.qp_key[i].first, myrdma.qp_key[i].second);
    if(rdma.pollCompletion(get<3>(myrdma.rdma_info[0][i]))){
        //cerr << "send success" << endl;
        tcp.send_msg("1", myrdma.sock_idx[i]);
    }
    else
        cerr << "send failed" << endl;
}
void D_RoCELib::rdma_write_with_imm(string msg, int i){
    RDMA rdma;
    
    if (msg.size() > 67108863)
        msg.replace(67108864,67108864, "\0");
    //msg[67108865] = NULL;
    strcpy(myrdma.send_buffer[i],msg.c_str());
    
    rdma.post_rdma_write_with_imm(get<4>(myrdma.rdma_info[0][i]), get<5>(myrdma.rdma_info[0][i]), myrdma.send_buffer[i], 
                                sizeof(myrdma.send_buffer[i]), myrdma.qp_key[i].first, myrdma.qp_key[i].second);
    if(!rdma.pollCompletion(get<3>(myrdma.rdma_info[0][i])))
        //cerr << "send success" << endl;
        cerr << "send failed" << endl;
    
}
void D_RoCELib::rdma_send_recv(int i){
    RDMA rdma;

    rdma.post_rdma_recv(get<4>(myrdma.rdma_info[1][i]), get<5>(myrdma.rdma_info[1][i]), 
                        get<3>(myrdma.rdma_info[1][i]),myrdma.recv_buffer[i], sizeof(myrdma.recv_buffer[i]));
    if(!rdma.pollCompletion(get<3>(myrdma.rdma_info[1][i])))
        cerr << "recv failed" << endl;
    else{
        cerr << strlen(myrdma.recv_buffer[i])/(1024*1024) <<"Mb data ";
        cerr << "receive success" << endl;
    }
}

void D_RoCELib::rdma_write_recv(int i){
    TCP tcp;
    //while(tcp.recv_msg(myrdma.sock_idx[i]) <= 0);
    //cerr << strlen(myrdma.recv_buffer[i])/(1024*1024) <<"Mb data ";
    //cerr << "recv success" << endl;
}

void D_RoCELib::rdma_send_msg(string opcode, string msg){
    if (opcode == "send_with_imm"){
        cerr << "rdma_send_with_imm run" <<endl;
        for(int i=0;i<myrdma.connect_num;i++){
            D_RoCELib::rdma_send_with_imm(msg, i);
        }
    }
    else if(opcode == "write"){
        cerr << "rdma_write run" << endl;
        for(int i=0;i<myrdma.connect_num;i++){
            D_RoCELib::rdma_write(msg, i);
        }
    }
    else if(opcode == "write_with_imm"){
        cerr << "write_with_imm_rdma run" <<endl;
        for(int i=0;i<myrdma.connect_num;i++){
            D_RoCELib::rdma_write_with_imm(msg, i);
        }
    }
    else if(opcode == "send"){
        cerr << "rdma_send run" <<endl;
        for(int i=0;i<myrdma.connect_num;i++){
            D_RoCELib::rdma_send(msg, i);
        }
    }
    else{
        cerr << "rdma_send_msg opcode error" << endl;
        exit(1);
    }
}
void D_RoCELib::rdma_recv_msg(string opcode, int i){
    if (opcode == "send_with_imm" || opcode == "write_with_imm" || opcode == "send"){
        D_RoCELib::rdma_send_recv(i);
    }
    else if(opcode == "write"){
        D_RoCELib::rdma_write_recv(i);
    }
    else{
        cerr << "rdma_recv_msg opcode error" << endl;
        exit(1);
    }
}
void D_RoCELib::recv_t(string opcode){
    std::vector<std::thread> worker;
    if (opcode == "send_with_imm" || opcode == "write_with_imm" || opcode == "send"){
        for(int i=0;i<myrdma.connect_num;i++){
            worker.push_back(std::thread(&D_RoCELib::rdma_send_recv,D_RoCELib(),i));
        }
    }
    else if(opcode == "write"){
        for(int i=0;i<myrdma.connect_num;i++){
            worker.push_back(std::thread(&D_RoCELib::rdma_write_recv,D_RoCELib(),i));
        }
    }
    else{
        cerr << "recv_t opcode error" << endl;
        exit(1);
    }
    for(int i=0;i<myrdma.connect_num;i++){
        worker[i].join();
    }
}
void D_RoCELib::initialize_connection(const char* ip, string server[], 
                                            int number_of_server, int Port, 
                                            char send[][buf_size], char recv[][buf_size]){
    TCP tcp;                                       
    tcp.connect_tcp(ip, server, number_of_server, Port);
    myrdma.send_buffer = &send[0];
    myrdma.recv_buffer = &recv[0];
    myrdma.connect_num = number_of_server - 1;

    int *clnt_socks = tcp.client_sock();
 
    for(int idx=0; idx < myrdma.connect_num+1; idx++){
        if(clnt_socks[idx]!=0)
            myrdma.sock_idx.push_back(idx);
    }

    int *serv_socks = tcp.server_sock();
 
    for(int idx=0; idx < myrdma.connect_num+1; idx++){
        if(serv_socks[idx]!=0)
            myrdma.sock_idx1.push_back(idx);
    }
}
void D_RoCELib::initialize_connection_vector(const char* ip, string server[], int number_of_server, int Port, vector<double> *send, vector<double> *recv, int num_of_vertex){
    TCP tcp;
    tcp.connect_tcp(ip, server, number_of_server, Port);
    myrdma.send = &send[0];
    myrdma.recv = &recv[0];
    myrdma.num_of_vertex = num_of_vertex;
     int *clnt_socks = tcp.client_sock();
 
    for(int idx=0; idx < myrdma.connect_num+1; idx++){
        if(clnt_socks[idx]!=0)
            myrdma.sock_idx.push_back(idx);
    }

    int *serv_socks = tcp.server_sock();
 
    for(int idx=0; idx < myrdma.connect_num+1; idx++){
        if(serv_socks[idx]!=0)
            myrdma.sock_idx1.push_back(idx);
    }
    //myRDMA::initialize_memory_pool();
    /*int n = num_of_vertex/(number_of_server-1); 
    partition=n;
    int n1 = num_of_vertex - n*(number_of_server-2);
    partition1=n1;
   
    //cout << partition << " " << partition1 << endl;
    for(int i=0;i<number_of_server-1;i++){
        //myrdma.send[i].resize(num_of_vertex);
        //myrdma.recv[i].resize(num_of_vertex);
        send_adrs.push_back(myrdma.send[i].data());
        recv_adrs.push_back(myrdma.recv[i].data());
    }
    rdma_info1[0].reserve(100000);
    rdma_info1[1].reserve(100000);*/
    
    myrdma.connect_num = number_of_server - 1;
}

void D_RoCELib::create_rdma_info(){
    RDMA rdma;
    TCP tcp;
    cerr << "Creating rdma info...   ";
    char (*buf)[buf_size];
    for(int j =0;j<2;j++){
        if(j==1){
            buf = &myrdma.recv_buffer[0];
            if(!buf){
                cerr << "Error please set_buffer() recv_buffer" << endl;
                exit(-1);
            }
        }
        else{
            buf = &myrdma.send_buffer[0];
            if(!buf){
                cerr << "\n";
                cerr << "Error please set_buffer() send_buffer" << endl;
                exit(-1);
            }
        }
        for(int i =0;i<myrdma.connect_num;i++){
            struct ibv_context* context = rdma.createContext();
            struct ibv_pd* protection_domain = ibv_alloc_pd(context);
            int cq_size = 0x10;
            struct ibv_cq* completion_queue = ibv_create_cq(context, cq_size, nullptr, nullptr, 0);
            struct ibv_qp* qp = rdma.createQueuePair(protection_domain, completion_queue);
            struct ibv_mr *mr = rdma.registerMemoryRegion(protection_domain, 
                                                    buf[i], sizeof(buf[i]));
            uint16_t lid = rdma.getLocalId(context, PORT);
            uint32_t qp_num = rdma.getQueuePairNumber(qp);
            myrdma.rdma_info[j].push_back(make_tuple(context,protection_domain,cq_size,
                                            completion_queue,qp,mr,lid,qp_num));
        }
    }
    cerr << "[ SUCCESS ]" << endl;
}
void D_RoCELib::roce_send_msg(string msg){
    TCP tcp;
    for(int i=0;i<myrdma.connect_num;i++){
        //tcp.send_msg(change(msg),myrdma.sock_idx[i]);
        tcp.send_vector(myrdma.send[i], myrdma.sock_idx[i]);
    }
}
void D_RoCELib::roce_recv_msg(int sock_idx, int idx){
    TCP tcp;
    //int str_len = tcp.recv_msg(sock_idx,myrdma.recv_buffer[idx],buf_size);
    //cout << "test" << endl;
    int str_len = tcp.recv_vector(sock_idx, myrdma.recv[idx],0);
}
void D_RoCELib::roce_recv_t(){
    std::vector<std::thread> worker;
    for(int i=0;i<myrdma.connect_num;i++){
        worker.push_back(std::thread(&D_RoCELib::roce_recv_msg,D_RoCELib(),myrdma.sock_idx[i],i));
        
    }
    for(int i=0;i<myrdma.connect_num;i++){
        worker[i].join();
    }
}
void D_RoCELib::roce_comm(string msg){
    thread snd_msg = thread(&D_RoCELib::roce_send_msg,D_RoCELib(),msg);
    D_RoCELib::roce_recv_t();

    snd_msg.join();
}
void D_RoCELib::roce_one_to_many_send_msg(string msg){
    D_RoCELib::roce_send_msg(msg);
}

void D_RoCELib::roce_one_to_many_recv_msg(){
    D_RoCELib::roce_recv_msg(myrdma.sock_idx[0],0);
}
void D_RoCELib::roce_many_to_one_send_msg(string msg){
    TCP tcp;
    //tcp.send_msg(change(msg),myrdma.sock_idx[0]);
    tcp.send_vector(myrdma.send[0], myrdma.sock_idx[0]);
}
void D_RoCELib::roce_many_to_one_recv_msg(){
    D_RoCELib::roce_recv_t();
}
void D_RoCELib::send_info_change_qp(){
    TCP tcp;
    RDMA rdma;
    //Send RDMA info
    for(int k = 0;k<2;k++){
        int *clnt_socks = tcp.client_sock();
        cerr << "Sending rdma info[" << k << "]... ";
        if(k==0){
            for(int idx=0; idx < myrdma.connect_num+1; idx++){
                if(clnt_socks[idx]!=0){
                    myrdma.sock_idx.push_back(idx);
                }
            }
        }
        for(int j=0;j<myrdma.connect_num;j++){
            std::ostringstream oss;

            if(k==0)
                oss << &myrdma.send_buffer[j];
            else
                oss << &myrdma.recv_buffer[j];
            
            tcp.send_msg(change(oss.str()+"\n"),myrdma.sock_idx[j]);
            tcp.send_msg(change(to_string(get<5>(myrdma.rdma_info[k][j])->length)+"\n"),myrdma.sock_idx[j]);
            tcp.send_msg(change(to_string(get<5>(myrdma.rdma_info[k][j])->lkey)+"\n"),myrdma.sock_idx[j]);
            tcp.send_msg(change(to_string(get<5>(myrdma.rdma_info[k][j])->rkey)+"\n"),myrdma.sock_idx[j]);
            tcp.send_msg(change(to_string(get<6>(myrdma.rdma_info[k][j]))+"\n"),myrdma.sock_idx[j]);
            tcp.send_msg(change(to_string(get<7>(myrdma.rdma_info[k][j]))+"\n"),myrdma.sock_idx[j]);
            
        }
        cerr << "[ SUCCESS ]" <<endl;
        //Read RDMA info
        map<string, string> read_rdma_info;
        cerr << "Changing queue pair...  ";
        for(int i=0;i<myrdma.connect_num;i++){
            read_rdma_info = tcp.read_rdma_info(myrdma.sock_idx[i]);
            //Exchange queue pair state
            rdma.changeQueuePairStateToInit(get<4>(myrdma.rdma_info[k^1][i]));
            rdma.changeQueuePairStateToRTR(get<4>(myrdma.rdma_info[k^1][i]), PORT, 
                                           stoi(read_rdma_info.find("qp_num")->second), 
                                           stoi(read_rdma_info.find("lid")->second));
                
            if(k^1==0){
                rdma.changeQueuePairStateToRTS(get<4>(myrdma.rdma_info[k^1][i]));
                myrdma.qp_key.push_back(make_pair(read_rdma_info.find("addr")->second,
                                                  read_rdma_info.find("rkey")->second));
            }
        }
        cerr << "[ SUCCESS ]" << endl;
    }
    cerr << "Completely success" << endl;
}
void D_RoCELib::rdma_comm(string opcode, string msg){
    thread snd_msg = thread(&D_RoCELib::rdma_send_msg,D_RoCELib(),opcode,msg);
    D_RoCELib::recv_t(opcode);

    snd_msg.join();
}

void D_RoCELib::rdma_one_to_many_send_msg(string opcode, string msg){
    D_RoCELib::rdma_send_msg(opcode, msg);
}

void D_RoCELib::rdma_one_to_many_recv_msg(string opcode){
    D_RoCELib::rdma_recv_msg(opcode);
}

void D_RoCELib::rdma_many_to_one_send_msg(string opcode, string msg){
    if (opcode == "send_with_imm"){
        cerr << "rdma_send_with_imm run" <<endl;
        D_RoCELib::rdma_send_with_imm(msg, 0);
    }
    else if(opcode == "write"){
        cerr << "rdma_write run" << endl;
        D_RoCELib::rdma_write(msg, 0);

    }
    else if(opcode == "write_with_imm"){
        cerr << "write_with_imm_rdma run" <<endl;
        D_RoCELib::rdma_write_with_imm(msg, 0);
    }
    else if(opcode == "send"){
        cerr << "rdma_send run" <<endl;
        D_RoCELib::rdma_send(msg, 0);
    }
    else{
        cerr << "rdma_many_to_one_send_msg opcode error" << endl;
        exit(1);
    }
}



