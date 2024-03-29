#include <vector>
#include <string>
#include <iostream>
#include <stdio.h>
#include "map"
#define buf_size 1048576*64 +1 

using namespace std;
class D_RoCELib{
    public:
        void rdma_send(string msg, int i);
        void rdma_send_with_imm(string msg, int i);
        void rdma_write(string msg, int i);
        void rdma_write_with_imm(string msg, int i);
        void rdma_send_recv(int i);
        void rdma_write_recv(int i);
        void rdma_send_msg(string opcode, string msg);
        void rdma_recv_msg(string opcode, int i=0);
        void recv_t(string opcode);
        void rdma_one_to_many_send_msg(string opcode, string msg);
        void rdma_one_to_many_recv_msg(string opcode);
        void roce_one_to_many_send_msg(string msg);
        void roce_one_to_many_recv_msg();
        void rdma_many_to_one_send_msg(string opcode, string msg);
        void rdma_many_to_one_recv_msg(string opcode);
        void roce_many_to_one_send_msg(string msg);
        void roce_many_to_one_recv_msg();
        void rdma_comm(string opcode, string msg);
        void create_rdma_info();
        void send_info_change_qp();
        void set_buffer(char send[][buf_size], char recv[][buf_size], int num_of_server);
        void initialize_connection(const char* ip, string server[], 
                                        int number_of_server, int Port,
                                        char send[][buf_size], char recv[][buf_size]);
        void initialize_connection_vector(const char* ip, string server[], int number_of_server, 
                                        int Port, vector<double> *send, vector<double> *recv, 
                                        int num_of_vertex);
        void roce_comm(string msg);
        void roce_send_msg(string msg);
        void roce_recv_msg(int sock_idx, int idx);
        void roce_recv_t();
        void exit_rdma();
    private:
        std::vector<tuple<struct ibv_context*, struct ibv_pd*, 
                        int, struct ibv_cq*,
                        struct ibv_qp*, struct ibv_mr*,
                        uint16_t, uint32_t>> rdma_info[2];
        std::vector<pair<string,string>> qp_key;
        char (*send_buffer)[buf_size];
        char (*recv_buffer)[buf_size];
        vector<double> *send;
        vector<double> *recv;
        vector<int> sock_idx;
        vector<int> sock_idx1;
        int connect_num;
        int num_of_vertex;
};