#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <sys/timeb.h>

#ifndef max
	#define max( a, b ) ( ((a) > (b)) ? (a) : (b) )
#endif

double get_time()
{
    struct timespec ts;
    double ret;

    clock_gettime(CLOCK_REALTIME, &ts);

            ret = ((((double)ts.tv_sec) * 1000) + (((double)ts.tv_nsec) / 1000000));
            //printf("ms %.20lf\n", ret);
            return ret;
}

/* Read sequence from a file to a char vector.
Filename is passed as parameter */

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

void printMatrix(char * seqA, char * seqB, int ** scoreMatrix, int sizeA, int sizeB)
{
	int i,j;
	
	//print header
	printf("Score Matrix:\n");
	printf("========================================\n");
	
	//print LCS score matrix allong with sequences

	printf("    ");
	printf("%5c   ", ' ');
	
	for (j=0;j<sizeA;j++)
		printf("%5c   ", seqA[j]);
	printf("\n");
	for (i=0; i<sizeB+1; i++)
	{
		if (i==0)
			printf("    ");
		else
			printf("%c   ", seqB[i-1]);
		for (j=0;j<sizeA+1;j++)
		{
			printf("%5d   ",scoreMatrix[i][j]);
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

int main(int argc, char ** argv)
{
	// sequence pointers for both sequences
	char *seqA, *seqB;

	// sizes of both sequences
	int sizeA, sizeB;
	
	#ifdef TIMEIO
	double time =get_time();
	#endif

	//read both sequences (filenames passed as command line arguments)
	seqA = read_seq(argv[1]);
	seqB = read_seq(argv[2]);


	//find out sizes
	sizeA = strlen(seqA);
	sizeB = strlen(seqB);
	
	// allocate LCS score matrix
	int ** scoreMatrix = allocateScoreMatrix(sizeA, sizeB);

	
	#ifdef TIMEIO
	time = get_time() - time;
	#endif


	//initialize LCS score matrix
	initScoreMatrix(scoreMatrix, sizeA, sizeB);
	
		
	//fill up the rest of the matrix and return final score (element locate at the last line and collumn)
	int score = LCS(scoreMatrix, sizeA, sizeB, seqA, seqB);

	/* if you wish to see the entire score matrix, 
	for debug purposes, define DEBUGMATRIX. */
	#ifdef DEBUGMATRIX
		printMatrix(seqA, seqB, scoreMatrix, sizeA, sizeB);
	#endif
	
	//print score
	printf("\nScore: %d\n", score);
        #ifdef TIMEIO
        double time2 =get_time();
        #endif
	//free score matrix
	freeScoreMatrix(scoreMatrix, sizeB);
	#ifdef TIMEIO
        time = get_time() - time2 + time;
        printf("I/O time = %lf\n", time);
        #endif
}

