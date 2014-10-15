#include <numericalInt.h>

int main(int argc, char *argv[]){

	if( argc != 2 ){
		printf("Error! Execution example: ./program 12\n");
	}

	int i, max;
	double sum;

	max = atoi(argv[1]);

	for(i =1; i <= max; i++){
		sum += f(i%10);
	}

	printf("Result: %3.4f\n", sum);
	return 0;
}
