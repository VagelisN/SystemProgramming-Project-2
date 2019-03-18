struct pool_listnode
{	
	pid_t pool_pid;
	int pipe_in;
	char* in_name;
	int pipe_out;
	char* out_name;
	int all_finished;
	struct pool_listnode* next;
};

struct job_info
{
	int job_state;
	int job_pid;
	time_t time_submited;
};

int pool(char* , char* , char* ,int,int);
void gettime(char**);

int pool_list_insert(struct pool_listnode** , struct pool_listnode** );
void pool_list_delete(struct pool_listnode* ,char*);