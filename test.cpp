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
#include "tcp.hpp"

#define df 0.85
#define MAX 100000
#define MAXX 50000
#define num_of_node 5
#define port 40145
#define server_ip "192.168.1.101"//"pod-a.svc-k8s-rdma"

string node[num_of_node] = {server_ip,"192.168.1.102","192.168.1.103","192.168.1.104","192.168.1.105"};//"pod-b.svc-k8s-rdma","pod-c.svc-k8s-rdma","pod-d.svc-k8s-rdma","pod-e.svc-k8s-rdma"};//,"192.168.1.102","192.168.1.103"};
string node_domain[num_of_node];
std::vector<std::vector<size_t>> graph;
std::vector<int> num_outgoing;
int num_of_vertex;
int start, end;
int edge;
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
            //if(rank == 0 && line_num%5000000 == 0)
            //   cerr << "[INFO]CREATE " << line_num << " LINES." << endl; 
            //if(line_num%500000 == 0)
                //cerr << "Create " << line_num << " lines" << endl;
		}
        
	} 
    
    else {
		cout << "Unable to open file" <<endl;
        exit(1);
	}
    num_of_vertex = graph.size();
    edge = line_num;
    delete infile;
}

int main(int argc, char** argv){
    TCP tcp;
    int rank, size, i ,j;
    int start, end;
    int a,b;
    long double network_time = 0;
    long double compute_time = 0;
    struct timespec begin1, end1 ;
    struct timespec begin2, end2 ;
    

    string my_ip= tcp.check_my_ip();
    

    /*TCP tcp;

    cout << "check my ip" << endl;
    my_ip = tcp.check_my_ip();
    cout << "finish! this pod's ip is " <<my_ip << endl;

    cout << "Changing domain to ip ..." << endl;
    for(int i = 0 ;i < num_of_node;i++){
        node[i]=tcp.domain_to_ip(node_domain[i]);
        cout << node_domain[i] << " ----> " << node[i] <<endl;
    }
    cout << "Success" << endl;*/

    //MPI Init
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    


    // Create Graph
    if(rank == 0){
        cout << "[INFO]IP: " << my_ip << endl;
        cout << "=====================================================" << endl;
        cout << "[INFO]CREATE GRAPH" << endl;
    }
    clock_gettime(CLOCK_MONOTONIC, &begin1);
    
    create_graph_data(argv[1],rank,argv[2]);      
    
    clock_gettime(CLOCK_MONOTONIC, &end1);
    long double create_graph_time = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;

    


    myRDMA myrdma;
    Pagerank pagerank;
    
    //D-RDMALib Init
    vector<double> send[num_of_node];
    vector<double> recv1[num_of_node];
    vector<double>* send_first = &send[1];
    vector<double>* send_end = &send[num_of_node-1];
    if(rank == 0){
        cout << "[INFO]FINISH CREATE GRAPH" << endl; // <<  create_graph_time << "s. " << endl;
        cout << "=====================================================" << endl;
        cout << "[INFO]NETWORK CONFIGURATION" << endl;
        myrdma.initialize_rdma_connection_vector(my_ip.c_str(),node,num_of_node,port,send,recv1,num_of_vertex);
        myrdma.create_rdma_info(send, recv1);
        myrdma.send_info_change_qp();
    }
    
    // graph partitioning
    double ve = edge/num_of_vertex +1 ;

    int recvcounts[size];
    int displs[size]; 
    int nn[num_of_node];
    int start_arr[num_of_node-1];
    start_arr[0] = 0;
    int end_arr[num_of_node-1];
    int temp = 0;
    size_t index = 0;
    int edge_part = ceil((edge/(num_of_node-1)));
    int vertex_part = ceil((num_of_vertex/(num_of_node-1)*ve));
    //cout << edge_part << endl;
    long long buffer_size = num_of_vertex * sizeof(double);
    long long buf_part = buffer_size/(num_of_node-1);
    int ttt = 1;
    cout << "ve: " << ve << endl;
    for(size_t i=0;i<num_of_vertex;i++){
        temp += num_outgoing[i];
        if( temp+ttt*ve < vertex_part+edge_part){//+ ttt + (ttt*sizeof(double))> edge_part+vertex_part+buf_part){
            //cout << i << ", " << temp - num_outgoing[i] + ttt << endl;
            temp = num_outgoing[i];
            end_arr[index] = i;
            if(index<num_of_node-1)
                start_arr[index+1] = i;
            //cout << "===========================" << endl;
            //cout << "start["<<index<<"]: " << start_arr[index] <<endl;
            //cout << "end["<<index<<"]: " << end_arr[index] <<endl;
            ttt=0;
            index++;
        }
        ttt++;
        if(index == num_of_node-2)
            break;
    }
    //cout << "===========================" << endl;
    end_arr[num_of_node-2] = num_of_vertex;
    //cout << "start["<<index<<"]: " << start_arr[index] <<endl;
    //cout << "end["<<index<<"]: " << end_arr[index] <<endl;
    //cout << "===========================" << endl;
    
    int div_num_of_vertex;
    if(my_ip != node[0]){
        for(int i=1;i<num_of_node;i++){
            if(node[i] == my_ip){
                div_num_of_vertex = end_arr[i-1] - start_arr[i-1];
                start = start_arr[i-1];
                end = end_arr[i-1];
            }
        }
        for(int i=0;i<num_of_node;i++){
            send[i].resize(div_num_of_vertex);
            recv1[i].resize(num_of_vertex, 1/num_of_vertex);
        }
        //cout << div_num_of_vertex << ", " << start << ", " << end << endl;
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
            start += end_arr[2];
            end += end_arr[2];
        }
        else if(my_ip == node[num_of_node-2]){
            start += end_arr[1];
            end += end_arr[1];
        }
        else if(my_ip == node[num_of_node-3]){
            start += end_arr[0];
            end += end_arr[0];
        }
        cout << "start, end: " << start <<", "<< end << endl;
    }
    else{
         for(int i=0;i<num_of_node;i++){
            int temp1 = end_arr[i] - start_arr[i];
            send[i].resize(num_of_vertex, 1/num_of_vertex);
            recv1[i].resize(temp1);
            nn[i] = temp1;
            //cout << "nn[i]: " <<nn[i] << endl;
        }
    }
    
    /*int div_num_of_vertex = num_of_vertex/(num_of_node-1);    
    if(my_ip == node[num_of_node-1])
        div_num_of_vertex = num_of_vertex - num_of_vertex/(num_of_node-1)*3;

    //cout << "start "<< endl;
    if(my_ip != node[0]){
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
    }*/
    int num_vertex = end-start;
    int num_edge = 0;
    for (int i = start; i < end; i++) {
        num_edge += num_outgoing[i];
    }

    cout << "\nVertex: " << num_vertex << endl;
    cout << "Edge: " << num_edge << endl << endl;
  // cout << "end" << endl;*/
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
    long double time3;
    long double mpi_time = 0;
    long double rdma_time = 0;
    //recv1[0].resize(num_of_vertex, 1/num_of_vertex);
    
    if(my_ip != node[0])
        div_send.resize(end-start);
    int send_size = div_send.size();
    double* send_buffer_ptr = div_send.data();
    double* recv_buffer_ptr = recv1[0].data();
    double* send_buf_ptr = send[0].data();
    check = 1;
    MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
    if(rank == 0){
        myrdma.rdma_comm("write_with_imm", "1");
    }
    MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
    
    clock_gettime(CLOCK_MONOTONIC, &begin2);
    //===============================================================================
    for(step =0;step<10000000;step++){
        
        if(rank == 0 || my_ip == node[0]){
            cout <<"================STEP "<< step+1 << "================" <<endl;
            
        }
        dangling_pr = 0.0;

        if(step!=0) {
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            if(my_ip != node[0]){
                for (size_t i=0;i<num_of_vertex;i++) {
                    if (num_outgoing[i] == 0)
                        dangling_pr += recv_buffer_ptr[i];   
                }
            }
            else{
                diff = 0;
                for (size_t i=0;i<num_of_vertex;i++) 
                    diff += fabs(prev_pr[i] - send_buf_ptr[i]);
            }
            clock_gettime(CLOCK_MONOTONIC, &end1);
            time3 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            compute_time+=time3;
        }
        //===============================================================================
        if(my_ip != node[0]){
            if(rank == 0)
                cout << "[INFO]COMPUTE PAGERANK" <<endl;
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            int idx;
            for(size_t i=start;i<end;i++){
                //cout << i << endl;
                //
                idx = i-start;
                double tmp = 0.0;
                const size_t graph_size = graph[i].size();
                const size_t* graph_ptr = graph[i].data();

                for(size_t j=0; j<graph_size; j++){
                    const size_t from_page = graph_ptr[j];
                    const double inv_num_outgoing = 1.0 / num_outgoing[from_page];

                    tmp += recv_buffer_ptr[from_page] * inv_num_outgoing;
                }
                send_buffer_ptr[idx] = (tmp + dangling_pr * inv_num_of_vertex) * df + df_inv * inv_num_of_vertex;
            }
            clock_gettime(CLOCK_MONOTONIC, &end1);
            time3 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            compute_time += time3;
            /*if(rank == 0)
                printf("%Lfs.\n", time3);*/
            //printf("%d: calc 수행시간: %Lfs.\n", rank, time3);
            //MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
            //---------------------------------------------------------------------------------------------------------------------
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            
            
            MPI_Allgatherv(send_buffer_ptr,send_size,MPI_DOUBLE,send_buf_ptr,recvcounts,displs,MPI_DOUBLE,MPI_COMM_WORLD);
            
            clock_gettime(CLOCK_MONOTONIC, &end1);
            time3 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            
            if(rank ==0){
                cout << "[INFO]START MPI_ALLGATHERV - SUCCESS ";
                cout << time3 << "s." <<endl;
                //printf("%Lfs\n", time3);
                network_time += time3;
                mpi_time += time3;
            }    
            //printf("%d: allgatherv 수행시간: %Lfs.\n", rank, time3);

            //long double time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            
            //MPI_Allgather(div_send.data(),div_send.size(),MPI_DOUBLE,send[0].data(),div_send.size(),MPI_DOUBLE,MPI_COMM_WORLD);
        }
        else{
            prev_pr = send[0];
        }
        //===============================================================================
        clock_gettime(CLOCK_MONOTONIC, &begin1);
        if(my_ip == node[0]){
            myrdma.recv_t("send");
            cout << "[INFO]START RECEIVE - SUCCESS" << endl;
            send[0].clear();

            for(size_t i=0;i<num_of_node-1;i++){
                size = nn[i];
                send[0].insert(send[0].end(),recv1[i].begin(),recv1[i].begin()+size);
            }   

            if(diff < 0.00001)
                send_buf_ptr[0] += 1; 
            
            fill(send_first, send_end, send[0]);
            cout << "[INFO]START AGGREGATE - SUCCESS" << endl;
        }
        else{
            if(rank == 0){
                cout << "[INFO]START SEND_RDMA - SUCCESS ";
                myrdma.rdma_write_vector(send[0],0);
            }
            
            
        }
        clock_gettime(CLOCK_MONOTONIC, &end1);
        long double time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
        if(rank == 0){
            network_time+=time1;
            rdma_time+=time1;
        }
        //printf("%d: send 수행시간: %Lfs.\n", rank, time1); 
        //===============================================================================
        if(my_ip == node[0]){
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            
            for(size_t i = 0; i<num_of_node-1;i++)
                myrdma.rdma_write_pagerank(send[0],i);
            cout << "[INFO]START SEND - SUCCESS" << endl;

            clock_gettime(CLOCK_MONOTONIC, &end1);
            time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            //printf("%d: send 수행시간: %Lfs.\n", rank, time1);
        }
        else{
            MPI_Request request;
            //std::vector<MPI_Request> requests;
            //MPI_Bcast(recv1[0].data(), recv1[0].size(), MPI_DOUBLE, 0, MPI_COMM_WORLD);

            if(rank == 0){
                cout << time1 << "s." <<endl;
                clock_gettime(CLOCK_MONOTONIC, &begin1);

                myrdma.rdma_recv_pagerank(0);
                cout << "[INFO]START RECEIVE_RDMA - SUCCESS ";

                //est_buf[0] = recv1[0];
                clock_gettime(CLOCK_MONOTONIC, &end1);
                time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
                cout << time1 << "s." << endl;
                rdma_time += time1;
                network_time += time1;
                //printf("%d: rdma_recv 수행시간: %Lfs.\n", rank, time1);
            }
            //MPI_Bcast(recv1[0].data(), recv1[0].size(), MPI_DOUBLE, 0, MPI_COMM_WORLD);
            //MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
            
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            if(rank == 0){
                cout << "[INFO]START MPI_BCAST - SUCCESS "; 
                for(size_t dest=1; dest<size; dest++){
                    MPI_Isend(recv_buffer_ptr, num_of_vertex, MPI_DOUBLE, dest, 32548, MPI_COMM_WORLD, &request);
                }
            }
            else{
                MPI_Irecv(recv_buffer_ptr, num_of_vertex, MPI_DOUBLE, 0, 32548, MPI_COMM_WORLD, &request);
                MPI_Wait(&request, MPI_STATUS_IGNORE);
            }
            
            MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
            //MPI_Bcast(recv_buffer_ptr, num_of_vertex, MPI_DOUBLE, 0, MPI_COMM_WORLD);
            clock_gettime(CLOCK_MONOTONIC, &end1);
           // MPI_Bcast(recv_buffer_ptr, num_of_vertex, MPI_DOUBLE, 0, MPI_COMM_WORLD);
            //clock_gettime(CLOCK_MONOTONIC, &end1);
            time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;

            if(rank == 0){
                cout << time1 << "s.\n" << endl;
                network_time += time1;
                mpi_time += time1;
                printf("COMPUTE PAGERANK:  %LFs.\n", compute_time);
                printf("NETWORK(MPI+RDMA): %Lfs.\n", network_time);
                printf("STEP %ld EXECUTION TIME: %Lfs.\n", step+1, compute_time + network_time);
                network_time = 0;
                compute_time = 0;
            }
            //printf("%d: mpi_broadcast 수행시간: %Lfs.\n", rank, time1);
            /*if(rank == 0){
                myrdma.rdma_recv_pagerank(0);
            }*/
            //double* recv_buffer_ptr = recv1[0].data();
            //cout << recv1[0].size() << endl;
            //cout << recv1[0].data() << endl;
            
            
            
        }
        clock_gettime(CLOCK_MONOTONIC, &end1);
        //time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
        //if(rank == 0)
         //   printf("%d: recv1 수행시간: %Lfs.\n", rank, time1);
        if(my_ip == node[0] && rank == 0)
            cout << "[INFO]DIFF: " <<diff << endl;
       
        
        if(diff < 0.00001 || recv1[0][0] > 1){
            break;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &end2);
    long double time2 = (end2.tv_sec - begin2.tv_sec) + (end2.tv_nsec - begin2.tv_nsec) / 1000000000.0;

    //===============================================================================
    
    if(my_ip != node[0] && rank == 0){
        cout << "=====================================================" << endl;
        double sum1 = accumulate(recv1[0].begin(), recv1[0].end(), -1.0);
        cout.precision(numeric_limits<double>::digits10);
        for(size_t i=num_of_vertex-200;i<num_of_vertex;i++){
            cout << "pr[" <<i<<"]: " << recv1[0][i] <<endl;
        }
        cout << "=====================================================" << endl;
        int important = 0;
        string result = "";
        double important_pr = recv1[0][0]-1;
        double tmp1 = important_pr;
        for (int i=1;i< num_of_vertex;i++){
            important_pr = max(important_pr, recv1[0][i]);
            if(tmp1 != important_pr){
                important = i;
                tmp1 = important_pr;
            }
        }

        cout << "[INFO]IMPORTANT VERTEX: " << important << "\n[INFO]" << important << "'S VALUE: "<<tmp1 << endl;
       // cout << "s = " <<round(sum1) << endl;
        //printf("총 수행시간: %Lfs.\n", time2);
    }
    if(rank == 0|| my_ip == node[0]){
        printf("[INFO]TOTAL EXECUTION TIME: %Lfs\n", time2);
        printf("[INFO]AVG MPI_TIME:  %Lfs.\n", mpi_time/62);
        printf("[INFO]AVG RDMA_TIME: %Lfs.\n", rdma_time/62);
        
        cout << "=====================================================" << endl;
    }
    MPI_Finalize();
}