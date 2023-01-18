#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <set>

#include "pthread.h"
#include "math.h"
#include "string.h"
using namespace std;


struct arguments {
	int numOfMappers;
	int numOfReducers;
	string nameOfFile;
};

struct myStructure {
  int tid; // id-ul thread-ului
  vector<string> *files; // fisierele de input dintr-un test
  vector<vector<vector<int>>> *outputMapper; // rezultatul actiunii tuturor mapper-ilor
  arguments argm; // argumentele citite in linia de comanda
};

pthread_mutex_t  mutex;
pthread_barrier_t barrier;

int binary_s(int n, int min, int max, int exp) {
	double middle = (min + max) /2;
	
	if(max < min) {
		// numarul n nu reprezinta o putere
	    return -1;
	}

	double x = pow(middle, exp);
	if(x == n) {
		// am gasit o baza
		return 1;
	}
	else if(x < n){
	    min = middle + 1;
	}
	else {
		max = middle - 1;
	}
	return binary_s(n, min, max, exp);
}

void findPerf(myStructure *str, string file) {

	int base;
    vector<int> num;

    ifstream myfile(file);
	int contor;
	myfile >> contor;
	int value;
	// adaug numerele din fisier in vectorul num
	while ( contor )
	{
		contor--;
		myfile >> value;
		num.push_back(value);
	}
	myfile.close();

	
	for(auto i : num) {
		for(int j = 2; j <= str->argm.numOfReducers + 1; j++) {
			int aux = sqrt(i);
			base = binary_s(i, 1, aux + 1, j);
			
			if(base != -1) {
				(*(str->outputMapper))[(*str).tid][j - 2].push_back(i);
				
			}
		}
	}

}
void *map(void *arg)
{
	myStructure thread_id = *(myStructure *)arg;
	string nameOfFile;

	while (thread_id.files->empty() != true)
	 {
        pthread_mutex_lock(&mutex);
		nameOfFile = thread_id.files->back();
		thread_id.files->pop_back();
        pthread_mutex_unlock(&mutex);

		findPerf((myStructure *) arg, nameOfFile);
		// thread-ul a terminat de interpretat fisierul
	}
	// mapper-ul asteapta la bariera   
    pthread_barrier_wait(&barrier);
	pthread_exit(NULL);
}

void *reduce(void *arg)
{
	myStructure thread_id = *(myStructure *)arg;

	// reducer asteapta la bariera   
	pthread_barrier_wait(&barrier);
	vector<vector<vector<int>>> list = *thread_id.outputMapper;
	set<int> listCompose;
	int exponent = thread_id.tid - thread_id.argm.numOfMappers;

	// crearea seturilor pentru fiecare exponent
    for (int i = 0; i < thread_id.argm.numOfMappers; i++)
	{
		for (auto j :  list[i][exponent])
		{
			listCompose.insert(j);
		}
		
	}
	
	ofstream my_file("out" + to_string(exponent + 2) + ".txt");
	my_file << listCompose.size();
	my_file.close();

	pthread_exit(NULL);
}

void get_args(int argc, char **argv, arguments *argm)
{
	if(argc < 3) {
		printf("Numar insuficient de parametri: ./program N P\n");
		exit(1);
	}

	argm->numOfMappers = atoi(argv[1]);
	argm->numOfReducers = atoi(argv[2]);
    argm->nameOfFile =  argv[3];
}

int main(int argc, char *argv[])
{
	int i;
	
	pthread_mutex_init(&mutex, NULL);
	arguments argm;
	get_args(argc, argv, &argm);
    int totalThreads = argm.numOfMappers + argm.numOfReducers;
	pthread_barrier_init(&barrier, NULL, totalThreads);

	// interpretarea fisierului de test
    ifstream myfile;
    myfile.open(argm.nameOfFile);
    string myline;
    getline(myfile, myline);
    int numOfFiles = stoi(myline);
    vector<string> files; // lista cu fisierele ce trebuie prelucrate
    
    if ( myfile.is_open() ) {
        for(int i=0; i<numOfFiles; i++)
        {
            getline (myfile, myline);
            files.push_back(myline);
        }
    }

	pthread_t tid[totalThreads];
	int thread_id[totalThreads];

	vector<myStructure> s(totalThreads);
	
	// initializare lista rezultat mapperi
	vector<vector<vector<int>>> outputMapper(argm.numOfMappers); 
	for (int i = 0; i < argm.numOfMappers; i++)
	{
		outputMapper[i] = vector<vector<int>>(argm.numOfReducers);
	}

	
	for (i = 0; i < totalThreads; i++) {
		thread_id[i] = i;
		s[i].tid = i;
		s[i].files = &files;
		s[i].argm = argm;
		
		s[i].outputMapper = &outputMapper;
        if(i < argm.numOfMappers) {
			// creare threaduri mapperi
		    pthread_create(&tid[i], NULL, map, &s[i]);
        }
        else {
			// creare threaduri reduceri
            pthread_create(&tid[i], NULL, reduce, &s[i]);
        }

	}

	for (i = 0; i < totalThreads; i++) {
		pthread_join(tid[i], NULL);
	}

	myfile.close();
	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);
	return 0;
}