#include <mpi.h>
#include <stdio.h>
#include<stdlib.h>
#include<time.h>
#include<string.h>
#include <omp.h>
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <math.h>
#include <algorithm>
#include <unistd.h>
#include <myRDMA.hpp>
#include <pagerank.hpp>
#include <numeric>

#define df 0.85
#define MAX 100000
#define MAXX 50000
#define num_of_node 5
#define port 40145
#define server_ip "192.168.0.100"

string node[num_of_node] = {server_ip,"192.168.0.101","192.168.0.102","192.168.0.104","192.168.0.106"};//,"192.168.1.102","192.168.1.103"};
std::vector<std::vector<size_t>> graph;
std::vector<int> num_outgoing;
int num_of_vertex;
int start, end;

using namespace std;


bool is_server(string ip){
  if(ip == server_ip)
    return true;
  return false;
}

template <class Vector, class T>
bool insert_into_vector(Vector& v, const T& t){
    typename Vector::iterator i = lower_bound(v.begin(), v.end(), t);
    if (i == v.end() || t < *i) {
        v.insert(i, t);
        return true;
    } else {
        return false;
    }
}
bool add_arc(size_t from, size_t to){
    vector<size_t> v;
    bool ret = false;
    size_t max_dim = max(from, to);

    if (graph.size() <= max_dim) {
        max_dim = max_dim + 1;
        
        graph.resize(max_dim);
        //pagerank.outgoing.resize(max_dim);
        if (num_outgoing.size() <= max_dim) {
            num_outgoing.resize(max_dim,0);
        }
    }
    //pagerank.graph[to].push_back(from);
    //cout << pagerank.graph[to] << endl;

    ret = insert_into_vector(graph[to], from);

    if (ret) {
        num_outgoing[from]++;
    }

    return ret;
}
void create_graph_data(string path, int rank, string del){
    //cout << "Creating graph about  "<< path<<"..."  <<endl;
    istream *infile;

    infile = new ifstream(path.c_str());
    size_t line_num = 0;
    string line;
	
	if(infile){
       
        while(getline(*infile, line)) {
            string from, to;
            size_t pos;
            if(del == " ")
                pos = line.find(" ");
            else
                pos = line.find("\t");

            from = line.substr(0,pos);
            to = line.substr(pos+1);
            add_arc(strtol(from.c_str(), NULL, 10),strtol(to.c_str(), NULL, 10));
            line_num++;
            if(rank == 0 && line_num%500000 == 0)
                cerr << "Create " << line_num << " lines" << endl; 
            //if(line_num%500000 == 0)
                //cerr << "Create " << line_num << " lines" << endl;
		}
        
	} 
    
    else {
		cout << "Unable to open file" <<endl;
        exit(1);
	}
    num_of_vertex = graph.size();
    delete infile;
}

int main(int argc, char** argv){
    int rank, size, i ,j;
    int start, end;
    int a,b;
    struct timespec begin1, end1 ;
    struct timespec begin2, end2 ;
    string my_ip(argv[1]);
    
    

    //MPI Init
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    


    // Create Graph
    //if(rank == 0)
    create_graph_data(argv[2],rank,argv[3]);
    
    
    /*for(int i=0;i<num_of_vertex;i++)
        MPI_Bcast(graph[i].data(), graph[i].size(), MPI_INT,0,MPI_COMM_WORLD);*/
    //MPI_Bcast(num_outgoing.data(), num_outgoing.size(), MPI_INT, 0, MPI_COMM_WORLD);
    myRDMA myrdma;
    Pagerank pagerank;
    
    //D-RDMALib Init
    //MPI_Bcast(&num_of_vertex, 1, MPI_INT, 0, MPI_COMM_WORLD);
    vector<double> send[num_of_node];
    vector<double> recv1[num_of_node];
    if(rank == 0 || my_ip == server_ip){
        myrdma.initialize_rdma_connection_vector(argv[1],node,num_of_node,port,send,recv1,num_of_vertex);
        myrdma.create_rdma_info();
        myrdma.send_info_change_qp();
    }

    // graph partitioning
    int recvcounts[size];
    int displs[size];
    int nn[num_of_node];

    int div_num_of_vertex = num_of_vertex/(num_of_node-1);    
    if(my_ip == node[num_of_node-1])
        div_num_of_vertex = num_of_vertex - num_of_vertex/(num_of_node-1)*3;

    //cout << "start "<< endl;
    if(my_ip != server_ip){
        //cout << "div_num_of_vertex: " <<div_num_of_vertex << endl;
        for(int i=0;i<size;i++){
            a = div_num_of_vertex/size*i;
            b = a + div_num_of_vertex/size;
            if(rank == i){
                start = a;
                end = b;
            }
            if(rank ==size-1 && rank == i){
                end = div_num_of_vertex;
            }
            displs[i] = a;
            recvcounts[i] = b-a;
            if(i ==size-1)
                recvcounts[i] = div_num_of_vertex-displs[i];

            //cout << "displs[" << i << "]: " <<displs[i] << endl;
            //cout << "recvcounts["<<i<<"]: " << recvcounts[i] << endl;
        }
        if(my_ip == node[num_of_node-1]){
            start += (num_of_vertex/(num_of_node-1))*3;
            end += (num_of_vertex/(num_of_node-1))*3;
        }
        else if(my_ip == node[num_of_node-2]){
            start += num_of_vertex/(num_of_node-1)*2;
            end += num_of_vertex/(num_of_node-1)*2;
        }
        else if(my_ip == node[num_of_node-3]){
            start += num_of_vertex/(num_of_node-1);
            end += num_of_vertex/(num_of_node-1);
        }
         //cout << "start, end: " << start <<", "<< end << endl;
        for(int i=0;i<num_of_node;i++){
            send[i].resize(div_num_of_vertex);
            recv1[i].resize(num_of_vertex, 1/num_of_vertex);
        }
    }
    else{
        for(int i=0;i<num_of_node;i++){
            send[i].resize(num_of_vertex, 1/num_of_vertex);
            recv1[i].resize(div_num_of_vertex);
            nn[i] = div_num_of_vertex;
        }
        int x = num_of_vertex - num_of_vertex/(num_of_node-1)*3;
        recv1[num_of_node-2].resize(x);

        nn[num_of_node-2] = x;
    }
  // cout << "end" << endl;
    int check;
    int check1[size];
    
    size_t step;
    double diff=1;
    double dangling_pr = 0.0;
    vector<double> prev_pr;
    double df_inv = 1.0 - df;
    double inv_num_of_vertex = 1.0 / num_of_vertex;
    //vector<double> gather_pr;
    //gather_pr.resize(num_of_vertex);
    vector<double> div_send;
    recv1[0].resize(num_of_vertex, 1/num_of_vertex);
    double* recv_buffer_ptr = recv1[0].data();

    if(my_ip != server_ip)
        div_send.resize(end-start);
    
    check = 1;
    MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
    if(rank == 0){
        myrdma.rdma_comm("write_with_imm", "1");
    }
    MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
    
    clock_gettime(CLOCK_MONOTONIC, &begin2);
    //===============================================================================
    for(step =0;step<10000000;step++){
        
        if(rank == 0 || my_ip == server_ip)
            cout <<"====="<< step+1 << " step=====" <<endl;
        dangling_pr = 0.0;
        //gather_pr = recv1[0];
        if(step!=0) {
            if(my_ip != server_ip){
                //recv1[0] = gather_pr;
                for (size_t i=0;i<num_of_vertex;i++) {
                    if (num_outgoing[i] == 0)
                        dangling_pr += recv1[0][i];   
                }
            }
            else{
                diff = 0;
                for (size_t i=0;i<num_of_vertex;i++) 
                    diff += fabs(prev_pr[i] - send[0][i]);
            }
        }
        //===============================================================================
        if(my_ip != server_ip){
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            for(size_t i=start;i<end;i++){
                //cout << i << endl;
                //
                double tmp = 0.0;
                const size_t graph_size = graph[i].size();
                const size_t* graph_ptr = graph[i].data();

                for(size_t j=0; j<graph_size; j++){
                    const size_t from_page = graph_ptr[j];
                    const double inv_num_outgoing = 1.0 / num_outgoing[from_page];

                    tmp += recv_buffer_ptr[from_page] * inv_num_outgoing;
                }
                div_send[i-start] = (tmp + dangling_pr * inv_num_of_vertex) * df + df_inv * inv_num_of_vertex;
            }
            clock_gettime(CLOCK_MONOTONIC, &end1);
            long double time3 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            printf("%d: calc 수행시간: %Lfs.\n", rank, time3);
            
            MPI_Allgatherv(div_send.data(),div_send.size(),MPI_DOUBLE,send[0].data(),recvcounts,displs,MPI_DOUBLE,MPI_COMM_WORLD);
            

            long double time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            
            //MPI_Allgather(div_send.data(),div_send.size(),MPI_DOUBLE,send[0].data(),div_send.size(),MPI_DOUBLE,MPI_COMM_WORLD);
        }
        else{
            prev_pr = send[0];
        }
        //===============================================================================
        clock_gettime(CLOCK_MONOTONIC, &begin1);
        if(my_ip == server_ip){
            myrdma.recv_t("send");
            cout << "recv1 success" << endl;
            send[0].clear();

            for(size_t i=0;i<num_of_node-1;i++){
                size = nn[i];
                send[0].insert(send[0].end(),recv1[i].begin(),recv1[i].begin()+size);
            }   

            if(diff < 0.00001)
                send[0][0] += 1; 
            
            fill(&send[1], &send[num_of_node-1], send[0]);
        }
        else{
            if(rank == 0){
                myrdma.rdma_write_vector(send[0],0);
                //cout << "send success" << endl;
            }
            
            
        }
        clock_gettime(CLOCK_MONOTONIC, &end1);
        long double time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
        //if(rank == 0)
            //printf("%d: send 수행시간: %Lfs.\n", rank, time1); 
        //===============================================================================
        clock_gettime(CLOCK_MONOTONIC, &begin1);
        if(my_ip == server_ip){
             for(size_t i = 0; i<num_of_node-1;i++)
                myrdma.rdma_write_pagerank(send[0],i);
        }
        else{
            MPI_Request request;
            if(rank == 0){
                myrdma.rdma_recv_pagerank(0);
                //gather_pr = recv1[0];
                /*for(size_t dest=1; dest<size; dest++){
                    MPI_Send(recv_buffer_ptr, num_of_vertex, MPI_DOUBLE, dest, 32548, MPI_COMM_WORLD);
                }*/
                cout << "recv_success" << endl;
                cout << "Broadcasting..." << endl;
            }
            /*else{
                MPI_Recv(recv_buffer_ptr, num_of_vertex, MPI_DOUBLE, 0, 32548, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
            }*/
            MPI_Bcast(send[0].data(), send[0].size(), MPI_DOUBLE, 0, MPI_COMM_WORLD);
        }
        clock_gettime(CLOCK_MONOTONIC, &end1);
        //time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
        //if(rank == 0)
         //   printf("%d: recv1 수행시간: %Lfs.\n", rank, time1);
        if(my_ip == server_ip && rank == 0)
            cout << "diff: " <<diff << endl;
        
        if(diff < 0.00001 || recv1[0][0] > 1){
            break;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &end2);
    long double time2 = (end2.tv_sec - begin2.tv_sec) + (end2.tv_nsec - begin2.tv_nsec) / 1000000000.0;

    //===============================================================================
    
    if(my_ip != server_ip && rank == 0){
        double sum1 = accumulate(recv1[0].begin(), recv1[0].end(), -1.0);
        cout.precision(numeric_limits<double>::digits10);
        for(size_t i=num_of_vertex-200;i<num_of_vertex;i++){
            cout << "pr[" <<i<<"]: " << recv1[0][i] <<endl;
        }
        cerr << "s = " <<sum1 << endl;
        //printf("총 수행시간: %Lfs.\n", time2);
    }
    if(rank == 0|| my_ip == server_ip)
        printf("총 수행시간: %Lfs.\n", time2);
    MPI_Finalize();
}