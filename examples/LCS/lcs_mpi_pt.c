#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>

#ifndef max
#define max( a, b ) ( ((a) > (b)) ? (a) : (b) )
#endif


/* Read sequence from a file to a char vector.
Filename is passed as parameter */

struct lcsData {
	MPI_Status * Stat;
	int numtasks;
	int rank;
	int wcluster,hcluster; //cluster matrix width and height
	int icluster,jcluster; // coordinates in cluster matrix

	int wchunk, hchunk; // chunk matriz width and height
    int wchunk2, hchunk2; // chunk matriz width and height

	// sequence pointers for both sequences
	char *seqA, *seqB;

	// sizes of both sequences
	int sizeA, sizeB;

	int chunk;

	int ** scoreMatrix; //score matrix
   
};

typedef struct {
	int ** scoreMatrix; 
	sem_t ** semMatriz; 
	int sizeA;
	int sizeB;
    int sizeA2;
	int sizeB2;
	int sizeRowSem;
	int sizeColumnSem;
	int chunk;
	int block;
	int icluster,jcluster; // coordinates in cluster matrix
	char * seqA, *seqB;
} param;

char* read_seq(char *fname){
//file pointer
FILE *fseq=NULL;
//sequence size
long size=0;
//sequence pointer
char *seq=NULL;
//sequence index
int i=0;

//open file
fseq=fopen(fname,"rt");
if(fseq==NULL){
	printf("Error reading file %s\n", fname);
	exit(1);
}

//find out sequence size to allocate memory afterwards	
fseek (fseq , 0L , SEEK_END);
size=ftell(fseq);
rewind (fseq);

//allocate memory (sequence)
seq=(char *) calloc(size+1,sizeof(char));
if(seq==NULL){
	printf("Erro allocating memory for sequence %s.\n", fname);
	exit(1);
}

//read sequence from file
while(!feof(fseq)){
	seq[i]=fgetc(fseq);
	if ((seq[i]!='\n') && (seq[i]!=EOF))
		i++;
	}
	//insert string terminator
	seq[i]='\0';

	//close file
	fclose(fseq);

	//return sequence pointer
	return seq;
}

int ** allocateScoreMatrix(int sizeA, int sizeB)
{
	int i;
	//Allocate memory for LCS score matrix    
	int ** scoreMatrix = (int **) malloc((sizeB+1)*sizeof(int *));
	for (i=0; i<(sizeB+1); i++)
		scoreMatrix[i]=(int *) malloc((sizeA+1)*sizeof(int));
	return scoreMatrix;
}

sem_t ** allocateSemMatrix(int sizeRow, int sizeColumn)
{
	int i;
	//Allocate memory for LCS score matrix    
    	sem_t ** semMatriz = (sem_t **) malloc((sizeRow)*sizeof(sem_t *));
    	for (i=0; i<(sizeRow); i++)
		semMatriz[i]=(sem_t *) malloc((sizeColumn)*sizeof(sem_t));	

	return semMatriz;
}

void initScoreMatrix(int ** scoreMatrix, int sizeA, int sizeB)
{
	int i, j;
	//Fill first line of LCS score matrix with zeroes
	for (j=0; j<(sizeA+1); j++)
		scoreMatrix[0][j]=0;

	//Do the same for the first collumn
	for (i=1; i<(sizeB+1); i++)
		scoreMatrix[i][0]=0;
}

void initSemMatrix(sem_t ** semMatriz, int sizeRow, int sizeColumn)
{
	int i, j;

	for (i=0; i<(sizeRow); i++){
		for (j=0; j<(sizeColumn); j++){
			sem_t sem;
			
			if ((i == 0) && (j == 0)){	
				sem_init(&sem, 0, 1);
			}else{
				sem_init(&sem, 0, 0);			
			}	

			semMatriz[i][j] = sem;
		}		

	}
}

void * LCS_thread(void * args){
	param * par = (param *)args;	
	int i, j, begin_i, begin_j, end_i, end_j, x, y;
	
	//Definindo as coordenadas da matriz de semáforos
	x = par->block/par->sizeColumnSem;
	y = par->block % par->sizeColumnSem;
	
	//Executando wait no semáforo
	if ((x > 0) && (y > 0) ){				
		sem_wait(&par->semMatriz[x][y]);
	}
			
	sem_wait(&par->semMatriz[x][y]);	

	//Intervalo da cordenada da linha que vai variar na thread
	/* Se a coordenada obtida no end_i for maior que o tamanho do texto que defini a quantidade linha da matriz, então o tamanhao máximo é o tamanho desse texto.
	   O '+ 1' no begin_i e na última instrução do end_i é feito porque a coluna e a linha de índice zero são auxiliares no algoritmo LCS.	
	*/
	begin_i = (x * par->chunk) + 1;
	end_i = (x + 1) * par->chunk;	
	end_i = (end_i > par->sizeB)? par->sizeB + 1 : end_i + 1;

	//Intervalo da cordenada da coluna que vai variar na thread
	begin_j = (y * par->chunk) + 1;	
	end_j = (y + 1) * par->chunk;
	end_j = (end_j > par->sizeA) ? par->sizeA + 1: end_j + 1;
	
	//Processamento da Thread
	for(i = begin_i; i < end_i; i++){
		for(j = begin_j; j < end_j; j++){
			if (par->seqA[par->jcluster*par->sizeA2 +j-1] == par->seqB[par->icluster*par->sizeB2+i-1])
			{ 
				/* if elements in both sequences match, 
				the corresponding score will be the score from
				previous elements + 1*/
				par->scoreMatrix[i][j] = par->scoreMatrix[i-1][j-1]+1;
			}
			else
			{
				/* else, pick the maximum value (score) from left and upper elements*/
				par->scoreMatrix[i][j] = max(par->scoreMatrix[i-1][j], par->scoreMatrix[i][j-1]);
			}
		}
	}
	
	//Liberando o semáforo dos vizinhos. 
	if (y != (par->sizeColumnSem - 1)){
		sem_post(&par->semMatriz[x][y + 1]);	
	}

	if (x != (par->sizeRowSem - 1)){
		sem_post(&par->semMatriz[x + 1][y]);
	}
	
	free(par);

}

void pMatrix(struct lcsData data)
{
	int i,j;

	//print header
	printf("Score Matrix %d:\n", data.rank);
	printf("========================================\n");

	//print LCS score matrix allong with sequences
	for (i=0; i<data.hchunk+1; i++)
	{
		for (j=0;j<data.wchunk+1;j++)
		{
			printf("%5d   ",data.scoreMatrix[i][j]);
		}
		printf("\n");
	}
	printf("========================================\n");
}

void freeScoreMatrix(int **scoreMatrix, int sizeB)
{
	int i;
	   	for (i=0; i<(sizeB+1); i++)
	free(scoreMatrix[i]);
	free(scoreMatrix);
}

void freeSemMatrix(sem_t ** semMatriz, int sizeRow)
{
	int i;
    	for (i=0; i<(sizeRow); i++)
		free(semMatriz[i]);
	free(semMatriz);
}

int LCS(struct lcsData data)
{

	//Calculando a quantidade de blocos
	/* Função ceil da biblioteca math.h arredonda sempre para cima*/
	int i, blocks, sizeRowSem, sizeColumnSem;
	sizeRowSem = ceil((double)data.hchunk/(double)data.chunk);
	sizeColumnSem = ceil((double)data.wchunk/(double)data.chunk);
	
	blocks = sizeRowSem * sizeColumnSem;
	sem_t ** semMatriz = allocateSemMatrix(sizeRowSem, sizeColumnSem);
	initSemMatrix(semMatriz, sizeRowSem, sizeColumnSem);

	//Criando as threads. Uma thread por bloco.	
	pthread_t * tid = (pthread_t *) malloc(blocks * sizeof(pthread_t));		
	
	//Se a quantidade de blocos for maior que o numero de thread que pode ser criado, o resultado dá errado.	
	for (i=0; i<blocks; i++){
		param * pT = (param *)malloc(sizeof(param));
		pT->scoreMatrix = data.scoreMatrix;
		pT->semMatriz = semMatriz;  
		pT->sizeA = data.wchunk;
		pT->sizeB = data.hchunk;
        pT->sizeA2 = data.wchunk2;
		pT->sizeB2 = data.hchunk2;
		pT->sizeRowSem = sizeRowSem;
		pT->sizeColumnSem = sizeColumnSem;
		pT->chunk = data.chunk;
		pT->block = i;
		pT->seqA = data.seqA;
		pT->seqB = data.seqB;				
		pT->icluster = data.icluster;
		pT->jcluster = data.jcluster;
		pthread_create(&tid[i], NULL, LCS_thread, pT);
	}
	
	for (i=0; i<blocks; i++)
		pthread_join(tid[i], NULL);


	
	freeSemMatrix(semMatriz, sizeRowSem);

	return data.scoreMatrix[data.hchunk][data.wchunk];
}

void init_ids(struct lcsData * data, char ** argv)
{

	//MPI_Comm_size(MPI_COMM_WORLD, &(data->numtasks));
	//MPI_Comm_rank(MPI_COMM_WORLD, &(data->rank));

	data->wcluster = atoi(argv[1]);

	data->icluster = data->rank / data->wcluster;
	data->jcluster = data->rank % data->wcluster;

	data->hcluster = data->numtasks/data->wcluster;

	data->wchunk = (data->sizeA / data->wcluster) + (((data->jcluster+1) == data->wcluster) * (data->sizeA % data->wcluster));
	data->hchunk = (data->sizeB / data->hcluster) + (((data->icluster+1) == data->hcluster) * (data->sizeB % data->hcluster));
    
    data->wchunk2 = (data->sizeA / data->wcluster);
    data->hchunk2 = (data->sizeB / data->hcluster);

	//printf("Rank: %d/%d - Cluster: %d x %d - Chunk: %d x %d\n",data->rank, data->numtasks, data->hcluster, data->wcluster, data->hchunk, data->wchunk);

}

void LCSreceive (struct lcsData data)
{
	int i;
	if (data.icluster > 0)
	{
		//printf("%d <- %d (%d)\n", data.rank, data.rank - data.wcluster, data.wchunk+1);
		MPI_Recv(data.scoreMatrix[0], data.wchunk+1, MPI_INT, data.rank-data.wcluster, 0, MPI_COMM_WORLD, data.Stat);
	}
	else
		for (i=0; i<(data.wchunk+1); i++)
		   data.scoreMatrix[0][i]=0;

	if (data.jcluster > 0)
	{
		//printf("%d <- %d (%d)\n", data.rank, data.rank - 1, data.hchunk+1);
		int * fromLeft = (int *) malloc(sizeof(int)*data.hchunk+1);
		MPI_Recv(fromLeft, data.hchunk+1, MPI_INT, data.rank-1, 0, MPI_COMM_WORLD, data.Stat);

		for (i=0; i<data.hchunk+1; i++)
			data.scoreMatrix[i][0] = fromLeft[i];

		free(fromLeft);
	}
	else
		for (i=0; i<(data.hchunk+1); i++)
			data.scoreMatrix[i][0]=0;
}

void LCSsend (struct lcsData data)
{
	if ((data.icluster+1) < data.hcluster)
	{
		//printf("%d -> %d (%d)\n", data.rank, data.rank + data.wcluster, data.wchunk+1);
		MPI_Send(data.scoreMatrix[data.hchunk], data.wchunk+1, MPI_INT, data.rank+data.wcluster, 0, MPI_COMM_WORLD);
	}
	if ((data.jcluster+1) < data.wcluster)
	{
		//printf("%d -> %d (%d)\n", data.rank, data.rank + 1, data.hchunk+1);
		int * toRight = (int *) malloc(sizeof(int)*data.hchunk+1);
		int i;
		for (i=0; i<data.hchunk+1; i++)
			toRight[i]=data.scoreMatrix[i][data.wchunk];

		MPI_Send(toRight, data.hchunk+1, MPI_INT, data.rank+1, 0, MPI_COMM_WORLD);
		free(toRight);
	}
}

int main(int argc, char ** argv)
{
	int numtasks, rank;
	//printf("teste");


	MPI_Status Stat;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &(numtasks));
	MPI_Comm_rank(MPI_COMM_WORLD, &(rank));

	struct lcsData data;

	data.Stat=&Stat;
	data.rank=rank;
	data.numtasks = numtasks;
	//printf("Files: %s %s\n",argv[2], argv[3]);

	//read both sequences (filenames passed as command line arguments)
	data.seqA = read_seq(argv[2]);
	data.seqB = read_seq(argv[3]);

	if (argc == 5){
		data.chunk = atoi(argv[4]);		
	}else{
		data.chunk = 1;
	}

	//find out sizes
	data.sizeA = strlen(data.seqA);
	data.sizeB = strlen(data.seqB);

	init_ids(&data, argv);

	// allocate LCS score matrix
	data.scoreMatrix = allocateScoreMatrix(data.wchunk, data.hchunk);

	//receive data from neighbors
	LCSreceive(data);


	//fill up the rest of the matrix and return final score (element locate at the last line and collumn)
	int score = LCS(data);
	//int score=0;
	// send data to neighbors
	LCSsend(data);


	//pMatrix(data);

	if (((data.icluster+1) == data.hcluster) && ((data.jcluster+1) == data.wcluster))
	{
		//print score - only the last node
		printf("\nScore: %d\n", score);
	}

	//free score matrix
	freeScoreMatrix(data.scoreMatrix, data.hchunk);
	MPI_Finalize();
	return 0;
}


