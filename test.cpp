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

#define df 0.85
#define MAX 100000
#define MAXX 50000
#define num_of_node 2
#define port 40145
#define server_ip "192.168.0.100"

string node[num_of_node] = {server_ip,"192.168.0.101"};//,"192.168.1.102","192.168.1.103"};
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

    delete infile;
}

int main(int argc, char** argv){
    int rank, size, i ,j;
    int start, end;
    string my_ip(argv[1]);
    vector<double> send[num_of_node];
    vector<double> recv[num_of_node];

    // Create Graph
    create_graph_data(argv[2],0,argv[3]);

    //MPI Init
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    myRDMA myrdma;
    Pagerank pagerank;
    
    

    //D-RDMALib Init
    if(rank == 1){
        myrdma.initialize_rdma_connection_vector(argv[1],node,num_of_node,port,send,recv,num_of_vertex);
        myrdma.create_rdma_info();
        myrdma.send_info_change_qp();
    }

    int div_num_of_vertex = num_of_vertex/num_of_node;
    
    if(my_ip == node[num_of_node-1])
        div_num_of_vertex = num_of_vertex - num_of_vertex/num_of_node;
    
    cout << div_num_of_vertex << endl;

    // graph partitioning
    for(int i=0;i<size;i++){
        if(rank == i){
            start = div_num_of_vertex/size*i;
            end = start + div_num_of_vertex/size;
        }
       
    }
    cout << start << ", " << end << endl;

    //
    /*for(i=0;i<size;i++){
        if(i==rank){
            start = (div/size)*i;
            end = start + div/size;
        } 
    }
    //start = 0;
    //end = MAX;
    double x = 0;
    struct timespec begin, end1 ;
    struct timespec begin1, end2;
    clock_gettime(CLOCK_MONOTONIC, &begin1);
    for(int k=0;k<30;k++){
        if(rank == 1)
            cout << "======================" << k << " step==========================" << endl;
        clock_gettime(CLOCK_MONOTONIC, &begin);
        for(i=start;i<end;i++){
            x = 0;
        
            for(j=0;j<MAXX;j++)
                x+=j;
            a[i-start] = j;
        }
        MPI_Allgather(a.data(),a.size(),MPI_DOUBLE,send[0].data(),a.size(),MPI_DOUBLE,MPI_COMM_WORLD);
        if(rank ==1)
            myrdma.rdma_comm("write_with_imm", "0");
        clock_gettime(CLOCK_MONOTONIC, &end1);
        long double time = (end1.tv_sec - begin.tv_sec) + (end1.tv_nsec - begin.tv_nsec) / 1000000000.0;
        if(rank ==1 || rank == 0){
            cout << k <<" step calc finish" << endl;
            printf("수행시간: %Lfs.\n", time);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &end2);
    long double time = (end2.tv_sec - begin1.tv_sec) + (end2.tv_nsec - begin1.tv_nsec) / 1000000000.0;
    if(rank == 0)
        cout << "총 수행시간: "<<time <<"s." << endl;*/
    
    MPI_Finalize();
}