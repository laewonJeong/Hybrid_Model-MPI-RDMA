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

#define MAX 100000
#define MAXX 50000
#define num_of_node 2
#define port 40145
#define server_ip "192.168.0.100"

string node[num_of_node] = {server_ip,"192.168.0.101"};//,"192.168.1.102","192.168.1.103"};
using namespace std;
//using namespace stdext;

bool is_server(string ip){
  if(ip == server_ip)
    return true;
  return false;
}

using namespace std;

int main(int argc, char** argv){
    int rank, size, i ,j;
    int start, end;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    vector<double> a;
    vector<double> send[2];
    vector<double> recv[2];
    int div = MAX/num_of_node;
    a.resize(div/size);
    send[0].resize(div);
    recv[0].resize(div);
    send[1].resize(div);
    recv[1].resize(div);

    myRDMA myrdma;

    if(rank == 1){
        myrdma.initialize_rdma_connection_vector(argv[1],node,num_of_node,port,send,recv,div);
        myrdma.create_rdma_info();
        myrdma.send_info_change_qp();
    }
    else{
        sleep(1);
    }
    
    
    for(i=0;i<size;i++){
        if(i==rank){
            start = (div/size)*i;
            end = start + div/size;
        } 
    }
    //start = 0;
    //end = MAX;
    double x = 0;
    struct timespec begin, end1 ;
    clock_gettime(CLOCK_MONOTONIC, &begin);
    for(i=start;i<end;i++){
        x = 0;
        if(i%5000 == 0){
            cout << rank << ": " << i <<" vertex calc finish" << endl;
            cout << "================================================" << endl;
        }
        for(j=0;j<MAXX;j++)
            x+=j;
        a[i-start] = j;
    }
    MPI_Allgather(a.data(),a.size(),MPI_DOUBLE,send[0].data(),a.size(),MPI_DOUBLE,MPI_COMM_WORLD);
    if(rank ==1){
        myrdma.rdma_comm("write_with_imm", "0");
    }
    clock_gettime(CLOCK_MONOTONIC, &end1);
    long double time = (end1.tv_sec - begin.tv_sec) + (end1.tv_nsec - begin.tv_nsec) / 1000000000.0;

    printf("수행시간: %Lfs.\n", time);
    MPI_Finalize();
}