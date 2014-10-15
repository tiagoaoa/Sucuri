#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <math.h>
#include <omp.h>

#ifndef max
#define max( a, b ) ( ((a) > (b)) ? (a) : (b) )
#endif

#ifndef min
#define min(a, b) ( ((a) < (b)) ? (a) : (b) )
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

int LCS(int ** scoreMatrix, int sizeA, int sizeB, char * seqA, char *seqB)
{
	int i,j;
	for (i=1; i<sizeB+1; i++)
	{
		for (j=1;j<sizeA+1;j++)
		{
			if (seqA[j-1] == seqB[i-1]) 
			{ 
				/* if elements in both sequences match, 
				the corresponding score will be the score from
				previous elements + 1*/
				scoreMatrix[i][j] = scoreMatrix[i-1][j-1]+1;
			}
			else
			{
				/* else, pick the maximum value (score) from left and upper elements*/
				scoreMatrix[i][j] = max(scoreMatrix[i-1][j], scoreMatrix[i][j-1]);
			}
		}
	}
	return 	scoreMatrix[sizeB][sizeA];
}

int LCS_Wave(struct lcsData data)
{
	int i,j, x, y, x_priv, y_priv;
	int sizeRow, sizeColumn, sizePrev;

	//Calculando a quantidade de blocos
	/* Função ceil da biblioteca math.h arredonda sempre para cima*/
	int sizeRowBlocks, sizeColumnBlocks;
	sizeRowBlocks = ceil((double)data.hchunk/(double)data.chunk);
	sizeColumnBlocks = ceil((double)data.wchunk/(double)data.chunk);
	
	//Dimensão da tabela com as diagonais convertidas
	sizeRow = sizeRowBlocks + sizeColumnBlocks - 1;
	sizeColumn = min(sizeRowBlocks, sizeColumnBlocks);
	
	for (i=0; i<sizeRow; i++)
	{
		
		if (i < sizeRowBlocks){
			x = i+1;
			y = 1;
		}else{	
			x = sizeRowBlocks;
			y = i - x + 2;
		}
		
		/*Prevendo a quantidade de loops que será feito nas colunas de cada linha
		  Fórmulas: 
			Quantidade Loops para chegar no indice de linha = 0 (Linha auxiliar)
			indice convertido - n_loops_1 = 0 => n_loops_1 = indice convertido

			Quantidade Loops para chegar no indice de coluna > que o tamanho de coluna da matriz  
			indice convertido + n_loops_2 = (tamanho_coluna_matriz + 1) => n_loops_2 = tamanho_coluna_matriz + 1 - indice convertido
			
			n_loops = min(n_loops_1, n_loops_2)

			n_loops => número de loops previstos para iterar na linha com o índice da "tabela" convertida com as diagonais linearizadas
			
		*/
		sizePrev = min(x, sizeColumnBlocks + 1 - y);		
		
		//printf("%d: SizePrev: %d \n", i, sizePrev);

		#pragma omp parallel for private(x_priv, y_priv)
		for (j=0; j<sizePrev; j++)
		{
			
			x_priv = x - j;
			y_priv = y + j;
			
			int k, l, begin_k, begin_l, end_k, end_l;	

			begin_k = ((x_priv - 1) * data.chunk) + 1;
			end_k = ((x_priv) * data.chunk) + 1;
			end_k = (end_k > data.hchunk)? data.hchunk + 1: end_k;
			
			begin_l = ((y_priv - 1) * data.chunk) + 1;
			end_l = ((y_priv) * data.chunk) + 1;
			end_l = (end_l > data.wchunk)? data.wchunk + 1 : end_l;
			
			//printf("Inicio(%d, %d), Fim(%d, %d) \n", begin_k, begin_l, end_k, end_l);
		
			for (k = begin_k; k < end_k; k++){

				for (l = begin_l; l < end_l; l++){
					if (data.seqA[data.jcluster*data.wchunk2+l-1] == data.seqB[data.icluster*data.hchunk2+k-1])
					{ 
						/* if elements in both sequences match, 
						the corresponding score will be the score from
						previous elements + 1*/
						data.scoreMatrix[k][l] = data.scoreMatrix[k-1][l-1]+1;
					}
					else
					{
						/* else, pick the maximum value (score) from left and upper elements*/
						data.scoreMatrix[k][l] = max(data.scoreMatrix[k-1][l], data.scoreMatrix[k][l-1]);
					}

				}
			}
			//printf("%d: (x, y) = (%d, %d); local(x, y) = (%d, %d): %c - %c \n", omp_get_thread_num(), x, y,x_priv, y_priv, seqA[y_priv-1], seqB[x_priv-1]);

		}

	}
	
	return data.scoreMatrix[data.hchunk][data.wchunk];
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
    
    data->wchunk2 = (data->sizeA / data->wcluster) ;
	data->hchunk2 = (data->sizeB / data->hcluster) ;

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
	int score = LCS_Wave(data);
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

