#include <numericalInt.h>

double f(int n){

	int i;
	double sum = 0;

	for(i =1; i <= ((n % 10) * PESO); i++){
		sum += sqrt(i);
	}

	return sum;
}
