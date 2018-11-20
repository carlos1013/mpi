#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <limits.h>
#define MAXX 1000001

// MAX char table (ASCII)
#define MAX 256

// Boyers-Moore-Hospool-Sunday algorithm for string matching
int bmhs(char *string, int n, char *substr, int m) {

	int d[MAX];
	int i, j, k;

	// pre-processing
	for (j = 0; j < MAX; j++)
		d[j] = m + 1;
	for (j = 0; j < m; j++)
		d[(int) substr[j]] = m - j;

	// searching
	i = m - 1;
	while (i < n) {
		k = i;
		j = m - 1;
		while ((j >= 0) && (string[k] == substr[j])) {
			j--;
			k--;
		}
		if (j < 0)
			return k + 1;
		i = i + d[(int) string[i + 1]];
	}

	return -1;
}

FILE *fdatabase, *fquery, *fout;

void openfiles() {

	fdatabase = fopen("dna.in", "r+");
	if (fdatabase == NULL) {
		perror("dna.in");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r");
	if (fquery == NULL) {
		perror("query.in");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w");
	if (fout == NULL) {
		perror("fout");
		exit(EXIT_FAILURE);
	}

}

void closefiles() {
	fflush(fdatabase);
	fclose(fdatabase);

	fflush(fquery);
	fclose(fquery);

	fflush(fout);
	fclose(fout);
}

void remove_eol(char *line) {
	int i = strlen(line) - 1;
	while (line[i] == '\n' || line[i] == '\r') {
		line[i] = 0;
		i--;
	}
}

char *bases;
char *str;
/*
tags:
{10,11,12,13}: infos da string passada pelo processo 0
1: tag de controle de loop dos processos
0: resposta dos processos slaves ao processo master
*/
int main(int argc, char **argv) {

	int meu_rank, np, tag = 0;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &np);
	MPI_Comm_rank(MPI_COMM_WORLD, &meu_rank);
	printf("Estou vivissima\n");

	if(!meu_rank){
		bases = (char*) malloc(sizeof(char) * MAX);
		str = (char*) malloc(sizeof(char) * MAX);

		openfiles();

		char desc_dna[100], desc_query[100];
		char line[100];
		int i, k, found = 0, result,control;

		fgets(desc_query, 100, fquery);
		remove_eol(desc_query);
		while (!feof(fquery)) {
			printf("%s\n",desc_query);  //printa a query atual
			found = 0;   //inicializa a varivel found
			fprintf(fout, "%s\n", desc_query);
			// read query string
			fgets(line, 100, fquery);
			remove_eol(line);
			str[0] = 0;
			i = 0;
			do {
				strcat(str + i, line);
				if (fgets(line, 100, fquery) == NULL)
					break;
				remove_eol(line);
				i += 80;
			} while (line[0] != '>');
			strcpy(desc_query, line);

			// read database and search
			fseek(fdatabase, 0, SEEK_SET);
			fgets(line, 100, fdatabase);
			remove_eol(line);

			while (!feof(fdatabase)) {
				strcpy(desc_dna, line); //copia o nome do genoma

				bases[0] = 0;
				i = 0;
				fgets(line, 100, fdatabase);
				remove_eol(line);
				do {
					strcat(bases + i, line);
					if (fgets(line, 100, fdatabase) == NULL)
						break;
					remove_eol(line);
					i += 80;
				} while (line[0] != '>');

				result = INT_MAX;

				control = 1;
				for (k=1; k < np; k++){
					MPI_Send(&control, 1, MPI_INT, k, 1, MPI_COMM_WORLD);
				}
				int resultado_parcial;
				int tamanho_string = strlen(str);
				int tamanho_base = strlen(bases);
				char *send = (char*) malloc(sizeof(char)*MAX);
				for(k = 1; k < np; k++){
					int razao = ((int)(tamanho_base/(np-1)));
					int ini = (razao) * (k-1),fim;   //calcula o inicio da string a ser enviada
					if (np < (k-1)){             //se o processo for diferente do ultimo
						fim = (ini + razao + (tamanho_string - 1));
					}
					else{          //aloca o restante da string para o ultimo processo
						fim = tamanho_base;
					}
					//printf("%d %d\n",ini,fim);
					int g;
					for(g = ini; g < fim; g++)
						send[g-ini] = bases[g];
					send[fim-ini] = '\0';

					//strncpy(send, bases+ini, (fim-ini));     //copia a string para envio
					//printf("%s\n",send);

					int tamanho_send = fim - ini;
					MPI_Send(&tamanho_send, 1, MPI_INT, k, 10, MPI_COMM_WORLD);
					MPI_Send(&tamanho_string, 1, MPI_INT, k, 11, MPI_COMM_WORLD);
					MPI_Send(send, strlen(send), MPI_CHAR, k, 12, MPI_COMM_WORLD);
					MPI_Send(str, strlen(str), MPI_CHAR, k, 13, MPI_COMM_WORLD);
					MPI_Send(&ini, 1, MPI_INT, k, 14, MPI_COMM_WORLD);
				}
				for (k=1; k < np; k++){        //espera os resultados chegarem dos processos slaves
					MPI_Recv(&resultado_parcial, 1, MPI_INT, k, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					result = (resultado_parcial<result) ? (resultado_parcial) : (result);     //pega o menor valor dos slaves
				}
				if (result!=INT_MAX){           //caso exista um resultado
					printf("%s\n",desc_dna);  //printa o nome do genoma
					printf("%d\n", result);
					found = 1;
				}
				free(send);
			}
			if (!found) printf("NOT FOUND\n");       //caso eu rode todo o genoma e nao encontre nenhuma string
		}
		control = -1;
		for (k=1; k < np; k++){         //avisa aos processos slaves que nao existe mais processamento a ser feito
			MPI_Send(&control, 1, MPI_INT, k, 1, MPI_COMM_WORLD);
		}

		closefiles();

		free(str);
		free(bases);
	}
	else{
		int resultado, cont, t_string, t_substring, v_real;
		char * string_local = (char*) malloc(sizeof(char) * MAX);
		char * string_parcial = (char*) malloc(sizeof(char) * MAX);

		MPI_Recv(&cont, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
		while (cont!=-1){
			//as linhas abaixo ainda sao da primeira versao do codigo
			MPI_Recv(&t_string, 1, MPI_INT, 0, 10, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			MPI_Recv(&t_substring, 1, MPI_INT, 0, 11, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			MPI_Recv(string_local, t_string, MPI_CHAR, 0, 12, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			MPI_Recv(string_parcial, t_substring, MPI_CHAR, 0, 13, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
			MPI_Recv(&v_real, 1, MPI_INT, 0, 14, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);          //recebe a verdadeira posicao atual na string maior para retorno do valor correto
			
			resultado = bmhs(string_local, t_string, string_parcial, t_substring);
			resultado = (resultado==-1) ? (INT_MAX) : (resultado+v_real);

			MPI_Send(&resultado, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);          //devolve o resultado para o processo master
			MPI_Recv(&cont, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);        //recebe o inteiro para saber se ainda existe processamento a ser feito
		}
		free(string_local);
		free(string_parcial);
	}
	MPI_Finalize();
	return 0;
}