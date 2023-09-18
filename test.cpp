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
#include <mutex>
#include <queue>
#include <condition_variable>
#include <future>
#define df 0.85
#define MAX 100000
#define MAXX 50000
#define num_of_node 5
#define port 40145
#define server_ip "192.168.1.101"//"pod-a.svc-k8s-rdma"

string node[num_of_node] = {server_ip,"192.168.1.102","192.168.1.103","192.168.1.104","192.168.1.105"};//"pod-b.svc-k8s-rdma","pod-c.svc-k8s-rdma","pod-d.svc-k8s-rdma","pod-e.svc-k8s-rdma"};//,"192.168.1.102","192.168.1.103"};
string node_domain[num_of_node];

std::vector<int> num_outgoing;
int num_of_vertex;
int start, end;
int edge;
int max_edge = 0;
using namespace std;

bool is_server(string ip){
  if(ip == server_ip)
    return true;
  return false;
}

int main(int argc, char** argv){
    TCP tcp;
    Pagerank pagerank;
    myRDMA myrdma;
    int rank, size, i ,j;
    int start, end;
    int a,b;
    int argvv = stoi(argv[3]);
    long double network_time = 0;
    long double compute_time = 0;
    struct timespec begin1, end1 ;
    struct timespec begin2, end2 ;
    std::vector<std::vector<size_t>>* graph = new std::vector<std::vector<size_t>>();
    std::vector<std::vector<size_t>> sliced_graph; //= new std::vector<std::vector<size_t>>();
    std::vector<std::vector<size_t>> *slice_graph;

    vector<double> send[num_of_node];
    vector<double> recv1[num_of_node];
    vector<double>* send_first = &send[1];
    vector<double>* send_end = &send[num_of_node-1];
    int my_idx;
    

    string my_ip= tcp.check_my_ip();
    for(int i=1;i<num_of_node;i++){
        if(my_ip == node[i])
            my_idx = i-1;
    }
    //MPI Init=====================================================================
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Create Graph================================================================
    int recvcounts[size];
    int displs[size]; 
    int nn[num_of_node];
    if(rank == 0){
        
        cout << "[INFO]IP: " << my_ip << endl;
        cout << "=====================================================" << endl;
        cout << "[INFO]CREATE GRAPH" << endl;
    }
    
    clock_gettime(CLOCK_MONOTONIC, &begin1);
    pagerank.create_vertex_weight(argv[1],argv[2], num_outgoing, num_of_vertex, 
                                start, end, nn, num_of_node, size, node, my_ip, 
                                rank, displs, recvcounts, send, recv1);
    
    num_of_vertex = num_outgoing.size();

    //pagerank.create_graph(argv[1],argv[2],graph,num_outgoing);
    pagerank.create_sliced_graph(argv[1],argv[2],start, end, sliced_graph);
    slice_graph = &sliced_graph;

    clock_gettime(CLOCK_MONOTONIC, &end1);
    long double create_graph_time = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
    //sliced_graph = sliced_graph;//(sliced_graph.begin(),sliced_graph.end());
    //slice_graph = (*sliced_graph);
   // delete sliced_graph;
    //if(my_ip != node[0]){
    //    slice_graph = std::vector<std::vector<size_t>>((*sliced_graph).begin(),(*sliced_graph).end());
    //    delete sliced_graph;
    //}
    //Check Graph size==============================================================
    
    size_t innerVectorsSize = 0;
    for (const auto& innerVector : (*slice_graph)) {
        innerVectorsSize += innerVector.size() * sizeof(size_t);
    }
    size_t totalSize = innerVectorsSize;
    
    size_t outgoing_size = sizeof(size_t) * num_outgoing.size();
    
    if(rank == 0){
        cout << "[INFO]FINISH CREATE GRAPH " <<  create_graph_time << "s. " << endl;
        //cout << "[INFO]GRAPH MEMORY USAGE: " << totalSize << " byte." << endl;
        cout << "[INFO]OUT_E MEMORY USAGE: " << outgoing_size << " byte." << endl;
        cout << totalSize + outgoing_size << " byte."<<endl;
        cout << "=====================================================" << endl;
        cout << "[INFO]GRAPH PARTITIONING" << endl;
    }
    cout << "[INFO]"<<rank<<": GRAPH MEMORY USAGE: " << totalSize << " byte." << endl;
    //while(1){
//
    //}
    //graph partitioning=============================================================
    

    /*pagerank.graph_partition(graph, sliced_graph, num_outgoing, num_of_vertex,
                            start, end, nn, num_of_node, size, node, my_ip, rank, 
                            displs, recvcounts, send, recv1);*/

    //Delete Graph===================================================================
    
    //delete graph;
    if(my_ip == node[0]){
        num_outgoing.clear();
        num_outgoing.shrink_to_fit();
    }

    //Check sliced_graph size==========================================================
    
    size_t s = sizeof(sliced_graph); // 외부 벡터의 크기

    for (const auto& innerVector : (*slice_graph)) {
        s += innerVector.size() * sizeof(size_t); // 내부 벡터의 크기
    }

    //D-RDMALib Init===================================================================
    
    if(rank == 0){
        cout << "[INFO]FINISH GRAPH PARTITIONING" << endl; // <<  create_graph_time << "s. " << endl;
        cout << "[INFO]SLICED GRAPH MEMORY USAGE: " << s << " byte." << endl;
        cout << "=====================================================" << endl;
        cout << "[INFO]NETWORK CONFIGURATION" << endl;
        myrdma.initialize_rdma_connection_vector(my_ip.c_str(),node,num_of_node,port,send,recv1,num_of_vertex);
        myrdma.create_rdma_info(send, recv1);
        myrdma.send_info_change_qp();
    }
   
    int num_vertex = end-start;
    int num_edge =0;
    //cout << start << ", " << end <<endl;
    for(int i=start; i<end;i++){
        num_edge += num_outgoing[i];
    }
    //cout << "\nVertex: " << num_vertex << endl;
    //cout << "Edge: " << num_edge << endl << endl;
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
    
    long double time3;
    long double mpi_time = 0;
    long double rdma_time = 0;
    //recv1[0].resize(num_of_vertex, 1/num_of_vertex);

    vector<double> div_send;
    double* send_buf_ptr;// = send[0].data();
    //int send_size;
    if(my_ip != node[0] && size > 1){
        div_send.resize(end-start);
        send_buf_ptr = div_send.data();
        //send_size = div_send.size();
    }
    else if(my_ip != node[0] && size <= 1){
        send_buf_ptr = send[0].data();
    }

    if(my_ip == node[0]){
        send_buf_ptr = send[0].data();
    }
    //int send_size = div_send.size();
    //int send_size = div_send.size();
    //
    double* recv_buffer_ptr = recv1[0].data();
   
    

    check = 1;
    MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
    if(rank == 0){
        myrdma.rdma_comm("write_with_imm", "1");
    }
    MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
    
    /*int start_idx;
    if(rank == 0){
        start_idx = start;
        for (int dest = 1; dest < size; dest++) {
            MPI_Send(&start_idx, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        }
    } else {
        // 다른 프로세스는 Rank 0으로부터 데이터를 받습니다.
        MPI_Recv(&start_idx, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }*/
    


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
            //const size_t sg_size = sliced_graph.size();
            if(rank == 0)
                cout << "[INFO]COMPUTE PAGERANK" <<endl;
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            int idx;
            for(size_t i=start-start;i<end-start;i++){
                //cout << i << endl;
                //
                idx = i;
                double tmp = 0.0;
                const size_t graph_size = (*slice_graph)[i].size();
                const size_t* graph_ptr = (*slice_graph)[i].data();
                for(size_t j=0; j<graph_size; j++){
                    const size_t from_page = graph_ptr[j];
                    const double inv_num_outgoing = 1.0 / num_outgoing[from_page];
                    tmp += recv_buffer_ptr[from_page] * inv_num_outgoing;
                }
                send_buf_ptr[idx] = (tmp + dangling_pr * inv_num_of_vertex) * df + df_inv * inv_num_of_vertex;
            }
            clock_gettime(CLOCK_MONOTONIC, &end1);
            time3 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            compute_time += time3;
            cout << rank << ", " << time3 << endl;
            /*if(rank == 0)
                printf("%Lfs.\n", time3);*/
            //printf("%d: calc 수행시간: %Lfs.\n", rank, time3);
            //MPI_Allgather(&check, 1, MPI_INT, check1, 1, MPI_INT, MPI_COMM_WORLD);
            //---------------------------------------------------------------------------------------------------------------------
            clock_gettime(CLOCK_MONOTONIC, &begin1);
            
            if(size > 1)
                MPI_Allgatherv(send_buf_ptr,div_send.size(),MPI_DOUBLE,send[0].data(),recvcounts,displs,MPI_DOUBLE,MPI_COMM_WORLD);
            
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
                //std::vector<double>::iterator iterator = recv1[i].begin();
                send[0].insert(send[0].end(),make_move_iterator(recv1[i].begin()),make_move_iterator(recv1[i].begin() + size));
            }   
           
            if(diff < 0.00001)
                send_buf_ptr[0] += 1; 
            
            
            myrdma.rdma_write_pagerank(send[0], 0);
            
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
            std::vector<std::thread> worker;
            size_t i;
            for(i = 1; i<num_of_node-1;i++){
                worker.push_back(std::thread(&myRDMA::rdma_write_pagerank, &myrdma,send[0],i));
            }
            for(i=0;i<num_of_node-2;i++)
                worker[i].join();
            cout << "[INFO]START SEND - SUCCESS" << endl;
            clock_gettime(CLOCK_MONOTONIC, &end1);
            time1 = (end1.tv_sec - begin1.tv_sec) + (end1.tv_nsec - begin1.tv_nsec) / 1000000000.0;
            printf("%d: send 수행시간: %Lfs.\n", rank, time1);
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
            //printf("%d - COMPUTE PAGERANK:  %LFs.\n", rank,compute_time);
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