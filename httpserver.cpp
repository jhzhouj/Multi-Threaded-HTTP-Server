#include <netdb.h>
#include <time.h>
#include <dirent.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h> 
#include <inttypes.h> 
#include <stdbool.h> 
#include <netinet/ip.h>
#include <sys/stat.h>
#include <stdio.h>
#include <err.h>
#include <errno.h>
#include <ctype.h>
#include <fcntl.h>
#include <unistd.h> 
extern int errno;
#define ERROR -1
#define FILE_END 0
#define CREATED 201
#define OK 200
#define CLIENT_ERROR 400
#define BUFFER_SIZE 32768
#define SERVER_ERROR 500


/*
    The arguments used as a shared variables
*/
struct thread_args
{
    // the buffer queue
    struct queue *queue;
    // sync primitive for lock
    pthread_mutex_t *queue_mutex;
    // sync primitive to wakeup the worker
    pthread_cond_t *queue_cond;
};

/*
    Data Structure for buffer queue implmentation
*/
struct node
{
    int fd;            // the file descriptor it stores
    struct node *prev; // the prev node
    struct node *next; // the next node
};

/*
    ADT for buffer queue implmentation
*/
struct queue
{
    struct node *head; // the head
    struct node *tail; // the tail
};


/*
    operation to create the buffer queue
*/
struct queue *queue_create()
{
    struct queue *q = (struct queue *)malloc(sizeof(struct queue));
    q->head = NULL;
    q->tail = NULL;
    return q;
};

/*
    operation to put file descriptor into the buffer queue
*/
void queue_enqueue(struct queue *queue, int fd)
{
    struct node *data = (struct node *)malloc(sizeof(struct node));
    data->fd = fd;
    if (queue->head == NULL && queue->tail == NULL)
    {
        // nothing in the queue
        queue->head = data;
        queue->tail = data;
    }
    else
    {
        data->prev = queue->tail;
        queue->tail->next = data;
        queue->tail = data;
    }
}

/*
    operation to take first element from the buffer queue
*/
int queue_dequeue(struct queue *queue)
{
    struct node *ret_val = NULL;
    // only 1 element
    if (queue->head == queue->tail)
    {
        ret_val = queue->head;
        queue->head = NULL;
        queue->tail = NULL;
    }
    else
    {
        // more than 1 element
        ret_val = queue->head;
        queue->head = ret_val->next;
    }
    // see if queue is empty
    if (ret_val != NULL)
    {
        int val = ret_val->fd;
        free(ret_val);
        return val;
    }
    else
    {
        return -1;
    }
}

/*
    predicate to check if the queue is empty
*/
int queue_is_empty(struct queue *queue)
{
    return queue->head == NULL;
}

/*
    operation to desctroy the queue.
*/
void queue_destroy(struct queue *queue)
{
    // get rid of everything
    while (queue_dequeue(queue) != -1)
    {
    }
    // free the queue
    free(queue);
}


void *worker(void *in_args)
{
    // casting
    struct thread_args *args = (struct thread_args *)in_args;
    // run forever
    while (true)
    {
        // acquire lock to enter critical region
        pthread_mutex_lock(args->queue_mutex);
        while (queue_is_empty(args->queue))
        {
            // wait on notification, exit critical region
            pthread_cond_wait(args->queue_cond, args->queue_mutex);
            // enter critical region
        }
        int client_sockd = queue_dequeue(args->queue);
        // release the lock, exit critical region
        pthread_mutex_unlock(args->queue_mutex);

        if (client_sockd == -1)
        {
            fprintf(stderr, "ERROR: client_sockd = -1?\n");
            continue;
        }
        struct httpObject message;

        /*
         * 2. Read HTTP Message
         */
        read_http_response(client_sockd, &message);

        /*
         * 3. Process Request and send response
         */
        process_request(client_sockd, &message);
    }
}





int isValidFile(char *resName) {
	char validName[65] =
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

	if (strlen(resName) == 10 && strspn(resName, validName) == 10) {
		return 1;
	}
	return 0;
}



/*
   response based on the HTTP request 
   */
void http_response(int client_socket, int status_code) {
	if(status_code == ERROR){
		if(errno == 13){ // 403 forbidden
			dprintf(client_socket, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
		}else if(errno == 2){ //404 not found
			dprintf(client_socket, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
		}else{ // 500 server error
			dprintf(client_socket, "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
		}
	}
	else if(status_code == OK){
		dprintf(client_socket, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
	}
	else if(status_code == CREATED){ // 201 CREATED
		dprintf(client_socket, "HTTP/1.1 201 Create\r\nContent-Length: 0\r\n\r\n");
	}else if(status_code == CLIENT_ERROR){ //400 client error
		dprintf(client_socket, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
	}else if(status_code == SERVER_ERROR){ // 500 server error
		dprintf(client_socket, "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
	}
	return;
}



// Function to handle GET requests
void handleGet(int socket, char *resourceName) {
	
	int status_code = 0;
	
	if (strlen(resourceName) != 10){
		status_code = CLIENT_ERROR;
		http_response(socket, status_code);
		return;
	}


	int i = 0;
	while(resourceName[i] != '\0'){
		if((isalnum(resourceName[i]) == 0)){
			status_code = CLIENT_ERROR;
			http_response(socket, status_code); //error
			return;
		}
		i++;
	}

	
    int file = open(resourceName, O_RDONLY);
    if (access(resourceName, 0) == 0 && file == -1) {
        status_code = ERROR;
        http_response(socket, status_code); // 403 
    } else {
        
        
        int length = 0;

        if (file > 0) {
            struct stat statbuf;
            fstat(file, &statbuf); // to read file length
            if(statbuf.st_size == ERROR){ // if fstat fails
                status_code = ERROR;
                http_response(socket, status_code); //either 403 or 404 or 500
            }
            length = statbuf.st_size; // get the file length


            char* ok = (char*)"HTTP/1.1 200 OK\r\nContent-Length: ";
            write(socket, ok, strlen(ok));   



            char lenStr[12]; 
            sprintf(lenStr, "%d\r\n\r\n", length);
            write(socket, lenStr, strlen(lenStr));
            
            lseek(file, 0, SEEK_SET);

            char *nextBuffer = (char*) malloc(BUFFER_SIZE);
            bool readComplete = false;
            while (!readComplete) {
                int readFile = read(file, nextBuffer, BUFFER_SIZE);
                if (readFile == 0) {
                    readComplete = true;
                } else {
                    write(socket, nextBuffer, readFile);
                }
            }
            free(nextBuffer);
        } else {
            status_code = ERROR;
            http_response(socket, status_code); // 404
        }
    }
	 
}




// Function to handle PUT requests
void handlePut(int socket, char *resourceName, char *length) {
	
	int status_code = 0;

	if (strlen(resourceName) != 10){
		status_code = CLIENT_ERROR;
		http_response(socket, status_code);
		return;
	}


	int i = 0;
	while(resourceName[i] != '\0'){
		if((isalnum(resourceName[i]) == 0)){
			status_code = CLIENT_ERROR;
			http_response(socket, status_code); //error
			return;
		}
		i++;
	}
	
    int contentLen = 0;
	if (strcmp(length, "") != 0) {
		contentLen = atoi(length);
	}
	
    if (open(resourceName, O_RDONLY) == -1) {
        int file = open(resourceName, O_CREAT | O_RDWR, 0666);
        char *newBuff = (char*) malloc(BUFFER_SIZE);
        if (contentLen > 0) {
            while (contentLen > 0) {
                int clientFile = read(socket, newBuff, BUFFER_SIZE);
                write(file, newBuff, clientFile);
                contentLen -= clientFile;
            }
        } else {
            bool readComplete = false;
            while (!readComplete) {
                int clientFile = read(socket, newBuff, BUFFER_SIZE);
                if (clientFile == 0) {
                    readComplete = true;
                }
                write(file, newBuff, clientFile);
            }
        }
        free(newBuff);
        status_code = CREATED;     // 201
        http_response(socket, status_code); 
    } else {
        int file = open(resourceName, O_RDWR | O_TRUNC);
        char *newBuff = (char*) malloc(BUFFER_SIZE);
        if (contentLen > 0) {
            while (contentLen > 0) {
                int clientFile = read(socket, newBuff, BUFFER_SIZE);
                write(file, newBuff, clientFile);
                contentLen -= clientFile;
            }
        } else {
            bool readComplete = false;
            while (!readComplete) {
                int clientFile = read(socket, newBuff, BUFFER_SIZE);
                if (clientFile == 0) {
                    readComplete = true;
                }
                write(file, newBuff, clientFile);
            }
        }
        status_code = OK;
        http_response(socket, status_code);
        free(newBuff);
    }
	 
}




int copyFile(const char *fromFilePath, const char *toFilePath) {
	int fromFile = open(fromFilePath, O_RDONLY);
	if (access(fromFilePath, 0) == 0 && fromFile == -1) {
		return 0;
	} else {
		int toFile = open(toFilePath, O_RDONLY);
		if (toFile == -1) {
			toFile = open(toFilePath, O_CREAT | O_RDWR, 0666);
		} else {
			toFile = open(toFilePath, O_RDWR | O_TRUNC);
		}

		if (fromFile == -1 || toFile == -1) {
			return 0;
		}

		char *buffer = (char*) malloc(BUFFER_SIZE);
		bool readComplete = false;
		while (!readComplete) {
			int readFile = read(fromFile, buffer, BUFFER_SIZE);

			if (readFile == 0) {
				readComplete = true;
			} else {
				write(toFile, buffer, readFile);
			}
		}
		free(buffer);
		close(fromFile);
		close(toFile);

		return 1;
	}
}

void clearCurrentDirectory(){
	DIR *dirp;
	struct dirent *dp;
	struct stat statbuf;

	dirp = opendir(".");
	if (dirp == NULL) {
		return;
	}

	while ((dp = readdir(dirp)) != NULL) {
		lstat(dp->d_name, &statbuf);
		if (!S_ISDIR(statbuf.st_mode) && isValidFile(dp->d_name)) {
			//delete
			remove(dp->d_name);
		}
	}

	closedir(dirp);
}

void handleBackup(int socket, time_t timestamp) {
	int status_code = 0;
	char folderName[256];
	char targetFilePath[258];
	int status;

	DIR *dirp;
	struct dirent *dp;
	struct stat statbuf;

	sprintf(folderName, "backup-%ld", timestamp);
	status = mkdir(folderName, 0666);
	if (status == -1) {
		//folder already exists.
	}

	dirp = opendir(".");
	if (dirp == NULL) {
		status_code = ERROR;
        http_response(socket, status_code); // 404
		return;
	}

	if (access(".", 0) != 0) {
		status_code = ERROR;
        http_response(socket, status_code); // 403 
		return;
	}



	while ((dp = readdir(dirp)) != NULL) {

		lstat(dp->d_name, &statbuf);
		if (!S_ISDIR(statbuf.st_mode) && isValidFile(dp->d_name)) {
			sprintf(targetFilePath, "%s/%s", folderName, dp->d_name);
			copyFile(dp->d_name, targetFilePath);
		}
	}

	closedir(dirp);

	status_code = OK;
    http_response(socket, status_code); // 200
}

void handleRecovery(int socket, const char *timestamp) {
	char folderName[256];
	char fromFilePath[259];
	char targetFilePath[259];

	DIR *dirp;
	struct dirent *dp;
	struct stat statbuf;

    int status_code = 0;
	int countFile = 0;

	if (strlen(timestamp) == 0) {
		status_code = CLIENT_ERROR;
        http_response(socket, status_code); // 400
		return;
	}

    

	sprintf(folderName, "backup-%s", timestamp);
	//printf("Recovery from %s\n", timestamp);

	dirp = opendir(folderName);
	if (dirp == NULL) {
		//If no backup with that timestamp exists,
		//then an error 404 is returned.
		status_code = ERROR;
        http_response(socket, status_code); // 404
		return;
	}

	if (access(folderName, 0) != 0) {
		status_code = ERROR;
        http_response(socket, status_code); // 403 
		return;
	}


	while ((dp = readdir(dirp)) != NULL) {
		sprintf(fromFilePath, "%s/%s", folderName, dp->d_name);
		lstat(fromFilePath, &statbuf);
		if (!S_ISDIR(statbuf.st_mode) && isValidFile(dp->d_name)) {
			countFile++;
		}
	}
	closedir(dirp);

	
	if (countFile > 0) {
		dirp = opendir(folderName);
		clearCurrentDirectory();
		while ((dp = readdir(dirp)) != NULL) {
			sprintf(fromFilePath, "%s/%s", folderName, dp->d_name);
			lstat(fromFilePath, &statbuf);
			if (!S_ISDIR(statbuf.st_mode) && isValidFile(dp->d_name)) {
				sprintf(targetFilePath, "./%s", dp->d_name);
				copyFile(fromFilePath, targetFilePath);
				//printf("%s %s\n", fromFilePath, targetFilePath);
			}
		}

		closedir(dirp);
	}
	

	status_code = OK;
    http_response(socket, status_code); // 200
}

void handleRecoveryRecent(int socket) {
	char timestamp[256];

	DIR *dirp;
	struct dirent *dp;
	struct stat statbuf;

    int status_code = 0;
	dirp = opendir(".");
	if (dirp == NULL) {
		status_code = ERROR;
        http_response(socket, status_code); // 404
		return;
	}

	//find most recent backup
	strcpy(timestamp, "");
	while ((dp = readdir(dirp)) != NULL) {
		lstat(dp->d_name, &statbuf);
		if (S_ISDIR(statbuf.st_mode)
				&& strncmp("backup-", dp->d_name, 7) == 0) {
			if (strcmp(timestamp, "") == 0
					|| strcmp(dp->d_name + 7, timestamp) > 0) {
				strcpy(timestamp, dp->d_name + 7);
			}
		}
	}
	closedir(dirp);

	if (strcmp(timestamp, "") == 0) {
		//no backup at all
		status_code = ERROR;
        http_response(socket, status_code); // 404
		return;
	}

	handleRecovery(socket, timestamp);
}

void handleList(int socket) {
	char timestamp[256];
	char *buff;

	DIR *dirp;
	struct dirent *dp;
	struct stat statbuf;

    int status_code = 0;
	dirp = opendir(".");
	if (dirp == NULL) {
		status_code = ERROR;
        http_response(socket, status_code); // 404
		return;
	}

	buff = (char*) malloc(BUFFER_SIZE);
	strcpy(buff, "");
	while ((dp = readdir(dirp)) != NULL) {
		lstat(dp->d_name, &statbuf);
		if (S_ISDIR(statbuf.st_mode) && opendir(dp->d_name) != NULL
				&& strncmp("backup-", dp->d_name, 7) == 0
				&& access(dp->d_name, 0) == 0) {
			sprintf(timestamp, "%s\n", dp->d_name + 7);
			strcat(buff, timestamp);
		}
	}
	closedir(dirp);

	char *ok = (char*) "HTTP/1.1 200 OK\r\n";
	char *contentLength = (char*) "Content-Length: ";
	write(socket, ok, strlen(ok));
	write(socket, contentLength, strlen(contentLength));
	char lenStr[12]; //max num of digits for a 32 bit integer is 12
	int length = strlen(buff);
	sprintf(lenStr, "%d\r\n\r\n", length);
	write(socket, lenStr, strlen(lenStr));
	write(socket, buff, length);
	free(buff);
}






// Main handles socket creation and input parsing
int main(int argc, char *argv[]) {


    if (argc < 2)
    {
        fprintf(stdout, "Usage: ./httpserver <port> [-N num_threads] [-r]\n");
        exit(-1);
    }
    int number_of_threads = 1;

    char *port = argv[1];

    //read args

    for (int i = 0; i < argc; i++)
    {
        if (argv[i][0] == '-')
        {
            if (argv[i][1] == 'N')
            {
                i++;
                number_of_threads = atoi(argv[i]);
                fprintf(stdout, "Using %d threads\n", number_of_threads);
            }

            else
            {
                fprintf(stderr, "unknown flag %s\n", argv[i]);
            }
        }
        else
        {
            port = argv[i];
        }
    }
	
    //create shared variables
    struct thread_args args;

    // create buffer queue
    args.queue = queue_create();

    // create sync primitives for queue
    args.queue_mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(args.queue_mutex, NULL);
    args.queue_cond = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
    pthread_cond_init(args.queue_cond, NULL);


    //create worker threads

    pthread_t *pthread_pool;
    pthread_pool = (pthread_t *)malloc(sizeof(pthread_t) * number_of_threads);
    for (int i = 0; i < number_of_threads; i++)
    {
        pthread_create(&pthread_pool[i], NULL, worker, &args);
    }

	struct sockaddr_in server_address;
	memset(&server_address, 0, sizeof(server_address));
	server_address.sin_family = AF_INET;
	server_address.sin_port = htons(port);
	server_address.sin_addr.s_addr = htonl(INADDR_ANY);
	socklen_t addrlen = sizeof(server_address);
	int server_socket = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr client_address;
	socklen_t client_addrlength;
	
	
    
	int ret;
	ret = bind(server_socket, (struct sockaddr *) &server_address, addrlen);
    if (ret < 0) {
		return EXIT_FAILURE;
	}

	ret = listen(server_socket, 5);
	if (ret < 0) {
		return EXIT_FAILURE;
	}
	//if server_socket smaller than 0, send error
	if (server_socket < 0) {
		perror("socket error");
	}

	


	while (1) {
		int new_socket;
		if ((new_socket = accept(server_socket, &client_address, &client_addrlength)) < 0) {
			perror("accept failed");
			exit(EXIT_FAILURE);
		}

        // add job the the job queue
        pthread_mutex_lock(args.queue_mutex);
        // enqueue
        queue_enqueue(args.queue, client_sockd);
        // signal worker
        pthread_cond_signal(args.queue_cond);
        pthread_mutex_unlock(args.queue_mutex);


		char *buffer = (char*) malloc(BUFFER_SIZE);
		read(new_socket, buffer, BUFFER_SIZE);


		char* putLength = (char*)" ";
		char* contenthd;
		const char* contentString = "Content-Length:";
		contenthd = strstr(buffer, contentString);
		putLength = contenthd + 15;

	
		free(buffer);
		char *tokens = strtok(buffer, " ");
		char *get = (char*) "GET";
		char *put = (char*) "PUT";
		if (strncmp(get, tokens, 3) == 0) {
			tokens = strtok(NULL, " ");
			if (tokens[0] == '/') {
				memmove(tokens, tokens + 1, strlen(tokens));
			}

			//check if it's backup, recovery or list
			if (strcmp(tokens, "b") == 0) {
				//backup
				handleBackup(new_socket, time(NULL));
			} else if (strcmp(tokens, "r") == 0) {
				//recovery to recent
				handleRecoveryRecent(new_socket);
			} else if (strncmp(tokens, "r/", 2) == 0) {
				//recovery to specific timestamp
				handleRecovery(new_socket, tokens + 2);
			} else if (strcmp(tokens, "l") == 0) {
				//list all backups
				handleList(new_socket);
			} else {
				handleGet(new_socket, tokens);
			}
		} else if (strncmp(put, tokens, 3) == 0) {
			tokens = strtok(NULL, " ");
			if (tokens[0] == '/') {
				memmove(tokens, tokens + 1, strlen(tokens));
			}
			handlePut(new_socket, tokens, putLength);
		} else {

			int status_code = SERVER_ERROR;
			http_response(new_socket, status_code);
			

		}
		close(new_socket);
	}
	return 0;
}
