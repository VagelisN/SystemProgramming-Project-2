#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include "pool_list.h"
#define MESSAGESIZE 70

static int termination_signal=0;//flag for the pool

static void sig_handler(int sig)//the signal handler changes the termination_signal's value and the coord knows its time to shutdown
{
	if(sig==SIGTERM)termination_signal=1;
	fflush(stdout);
}

void gettime(char** buff)//function used to create the folder names
{
	time_t time1;
	struct tm *tm;
	time(&time1);
	tm=localtime(&time1);
	strftime((*buff),50,"%Y%m%d_%H%M%S",tm);
}


//just an insertion to a linked list
int pool_list_insert(struct pool_listnode** head, struct pool_listnode** last_node)
{
	if((*head)==NULL)
	{
		(*head)=malloc(sizeof(struct pool_listnode));
		(*head)->in_name=malloc(sizeof(char)*strlen("pool0in")+1);
		strcpy((*head)->in_name,"pool0in");
		(*head)->out_name=malloc(sizeof(char)*strlen("pool0out")+1);
		strcpy((*head)->out_name,"pool0out");
		(*head)->all_finished=0;
		(*head)->next=NULL;
		(*last_node)=(*head);
	}
	else
	{
		int poolnumber=1;
		char buff[25];
		struct pool_listnode *temp=(*head);
		while((*head)->next!=NULL)
		{
			poolnumber++;
			(*head)=(*head)->next;
		}
		(*head)->next=malloc(sizeof(struct pool_listnode));
		(*head)=(*head)->next;
		sprintf(buff,"pool%din",poolnumber);
		(*head)->in_name=malloc(sizeof(char)*strlen(buff)+1);
		strcpy((*head)->in_name,buff);
		sprintf(buff,"pool%dout",poolnumber);
		(*head)->out_name=malloc(sizeof(char)*strlen(buff)+1);
		strcpy((*head)->out_name,buff);
		(*head)->all_finished=0;
		(*head)->next=NULL;
		(*last_node)=(*head);
		(*head)=temp;
	}
	return 0;
}

//delete the linked list 
void pool_list_delete(struct pool_listnode* head,char* path)
{
	struct pool_listnode *temp;
	char tempbu[100];
	while(head!=NULL)
	{
		temp=head;
		head=head->next;
		close(temp->pipe_in);
		close(temp->pipe_out);
		if(path!=NULL)
		{
			sprintf(tempbu,"%s%s",path,temp->in_name);
			if(unlink(tempbu)==-1)perror("unlink");
			sprintf(tempbu,"%s%s",path,temp->out_name);
			if(unlink(tempbu)==-1)perror("unlink");

		}
		free(temp->in_name);
		free(temp->out_name);
		free(temp);
	}
}


/* the pool function checks in a loop the status of all its jobs in order to know if it can terminate
or to be ready to answer to the coord. When a command arrives it serves the command and keeps on checking 
the states of its jobs */
int pool(char* out_name, char* in_name, char* path,int jobs_pool,int numofjobs)
{
	int pipe_in,pipe_out,i,jobs=-1,fin=0,stat,job_stdout,job_stderr,jobs_at_start=numofjobs;
	pid_t child_pid;
	struct job_info *job_array=NULL;
	char buff[MESSAGESIZE],job_output[MESSAGESIZE],*token=NULL,*submit_args[25],pathbuff[100],*timebuff=NULL;
	static struct sigaction act;
	act.sa_handler=sig_handler;
	sigfillset(&(act.sa_mask));
	sigaction(SIGTERM , &act , NULL);
	sprintf(pathbuff,"%s%s",path,in_name);
	if ((pipe_in=open(pathbuff, O_RDONLY|O_NONBLOCK))< 0){perror("fifo  open");exit(1);}//open the pipe for read and the coord can now open its side for write
	sprintf(pathbuff,"%s%s",path,out_name);
	if ((pipe_out=open(pathbuff,O_WRONLY))<0){perror("fifo  open");exit(1);}//the coord has opened its side for read and this completes tha handshake
	job_array=malloc(jobs_pool*sizeof(struct job_info));//the pool keeps an array with information about its jobs 
	timebuff=malloc(50*sizeof(char));

	for(i=0;i<jobs_pool;i++)
	{
		job_array[i].job_state=0;
		job_array[i].job_pid=0;
	}
	while(1)
	{
		if(termination_signal==1)//this only happens if a shutdown command has sent a termination signal
		{
			int jobs_killed=0;
			for (i = 0; i < jobs_pool; i++)
			{
				if (job_array[i].job_state!= 0 && job_array[i].job_state!=3)//if the job is not finished 
				{
					if (waitpid(job_array[i].job_pid,&stat,WNOHANG)!=job_array[i].job_pid)
					{
						if(kill(job_array[i].job_pid,SIGTERM)!=0){perror("job kill\n");exit(2);}//kill it
						jobs_killed++;
					}
				}
			}
			sprintf(buff,"%d",jobs_killed);
			write(pipe_out,buff,MESSAGESIZE);
			//after the shutdown signal the pool closes so it needs to clean the allocated memory
			close(pipe_in);
			close(pipe_out);
			free(job_array);
			free(timebuff);
			return 1;
		}
		if(read(pipe_in,buff,MESSAGESIZE)>0)//the pool checks for a command 
		{
			printf("pool: coord said : %s\n",buff );
			token=strtok(buff," ");
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~submit~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			if (strcmp(token,"submit")==0)
			{
				numofjobs++;
				//create the folder
				gettime(&timebuff);
				sprintf(pathbuff,"%sjms_sdi1400123_%d_%d_%s",path,numofjobs,getpid(),timebuff);
				if(mkdir(pathbuff,0700)==-1){perror("mkdir");exit(4);}
				//create two fds to redirect the jobs stdout and stderr
				sprintf(job_output,"%s/stdout_%d",pathbuff,numofjobs);
				job_stdout= creat(job_output,0777);
				sprintf(job_output,"%s/stderr_%d",pathbuff,numofjobs);
				job_stderr=creat(job_output,0777);
				if ((child_pid=fork())!=0)//pool process
				{
					close(job_stdout);//close the fds that are not needed in the pool
					close(job_stderr);
					//update the array 
					jobs++;
					job_array[jobs].job_state=1;
					job_array[jobs].job_pid=child_pid;
					job_array[jobs].time_submited=time(NULL);
					//inform the coord
					sprintf(buff,"PID: %d",child_pid);
					write(pipe_out,buff,MESSAGESIZE);
				}
				else//job
				{
					i=0;
					while((token=strtok(NULL," "))!=NULL)//parse the command in an array so that it can be given to execvp
					{
						submit_args[i]=malloc(sizeof(char)*(strlen(token))+1);
						strcpy(submit_args[i],token);
						i++;
					}
					submit_args[i]=NULL;
					//this redirects the stderr and stdout to the fds created by the pool process (they were inherrited)
					dup2(job_stdout,1);
					dup2(job_stderr,2);
					if(execvp(submit_args[0],submit_args)<0)//if exec fails we need to clean the pools inherrited memory 
					{
						perror("exec error\n");
						i=0;
						while(submit_args[i]!=NULL){free(submit_args[i]);i++;}
						close(pipe_in);
						close(pipe_out);
						free(job_array);
						free(timebuff);
						return -1;
					}

				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~status~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"status")==0)
			{
				//just checks the status of the job and informs the coord
				int temp_id=atoi(strtok(NULL," "));
				if(job_array[temp_id-1].job_state==3||waitpid(job_array[temp_id-1].job_pid,&stat,WNOHANG)==job_array[temp_id-1].job_pid)
				{
					sprintf(buff,"JobID %d status: finished",(jobs_at_start+temp_id));
					write(pipe_out,buff,MESSAGESIZE);
				}
				else if(job_array[temp_id-1].job_state==2 && waitpid(job_array[temp_id-1].job_pid,&stat,WNOHANG)!=job_array[temp_id-1].job_pid)
				{
					sprintf(buff,"JobID %d status: suspended",(jobs_at_start+temp_id));
					write(pipe_out,buff,MESSAGESIZE);
				}
				else if(job_array[temp_id-1].job_state == 1 && waitpid(job_array[temp_id-1].job_pid,&stat,WNOHANG)!=job_array[temp_id-1].job_pid)
				{
					sprintf(buff,"JobID %d status: active (running for %d seconds)",(jobs_at_start+temp_id),(int)(time(NULL)-job_array[temp_id-1].time_submited));
					write(pipe_out,buff,MESSAGESIZE);
				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~status_all~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"status-all")==0)
			{
				for (i = 0; i < jobs_pool; i++)//checks the status of all the jobs and informs the coord 
				{
					if(job_array[i].job_state== 0);//if no job in this place just skip it
					else if(job_array[i].job_state==3||waitpid(job_array[i].job_pid,&stat,WNOHANG)==job_array[i].job_pid)
					{
						sprintf(buff,"JobID %d status: finished ",(jobs_at_start+i+1));
						write(pipe_out,buff,MESSAGESIZE);
					}
					else if(job_array[i].job_state==2 && waitpid(job_array[i].job_pid,&stat,WNOHANG)!=job_array[i].job_pid)
					{
						sprintf(buff,"JobID %d status: suspended ",(jobs_at_start+i+1));
						write(pipe_out,buff,MESSAGESIZE);
					}
					else if(job_array[i].job_state == 1 && waitpid(job_array[i].job_pid,&stat,WNOHANG)!=job_array[i].job_pid)
					{
						sprintf(buff,"JobID %d status: active (running for %d seconds) ",(jobs_at_start+i+1),(int)(time(NULL)-job_array[i].time_submited));
						write(pipe_out,buff,MESSAGESIZE);
					}
				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~suspend~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"suspend")==0 || strcmp(token,"resume")==0)
			{
				int sus_or_res=0;
				if(strcmp(token,"suspend")==0)sus_or_res=1;
				int temp_id=atoi(strtok(NULL," "));
				if(job_array[temp_id-1].job_state!=0 && job_array[temp_id-1].job_state!=3)//if the job is not terminated
				{
					if(waitpid(job_array[temp_id-1].job_pid,&stat,WNOHANG)!=job_array[temp_id-1].job_pid)
					{
						if(sus_or_res==1)//suspend 
						{
							if(kill(job_array[temp_id-1].job_pid,SIGSTOP)==-1)perror("suspend signal");//send stop signal 
							while(1)
							{
								if(waitpid(job_array[temp_id-1].job_pid ,&stat,WNOHANG|WUNTRACED)>0)
								{	
									if(WIFSTOPPED(stat))//wait to see that it was suspended
									{
										job_array[temp_id-1].time_submited=0;//update the array 
										job_array[temp_id-1].job_state=2;
										break;	
									}
								}
							}
						}
						else//resume
						{
							if(kill(job_array[temp_id-1].job_pid,SIGCONT)==-1)perror("resume signal");//just resume the job and update the array
							job_array[temp_id-1].job_state=1;
							job_array[temp_id-1].time_submited=time(NULL);
						}
					}
					if(sus_or_res==1)strcpy(buff,"suspended");
					else strcpy(buff,"resumed");
					write(pipe_out,buff,MESSAGESIZE);
				}
				else
				{
					if(sus_or_res==1)strcpy(buff,"suspended");
					else strcpy(buff,"resumed");
					write(pipe_out,buff,MESSAGESIZE);
				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~show_finished~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"show-finished")==0)
			{
				for (i = 0; i < jobs+1; i++)
				{

						if(job_array[i].job_state==3 || waitpid(job_array[i].job_pid,&stat,WNOHANG)==job_array[i].job_pid)
						{
							sprintf(buff,"JobID %d",(jobs_at_start+i+1));
							write(pipe_out,buff,MESSAGESIZE);
						}
					else
					{
						strcpy(buff,"skip");
						write(pipe_out,buff,MESSAGESIZE);
					} 
				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~show_active~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"show-active")==0)
			{
				for (i = 0; i < jobs_pool; i++)
				{
					if(job_array[i].job_state==1 && waitpid(job_array[i].job_pid,&stat,WNOHANG)!=job_array[i].job_pid)//chech that the job is really active (and not suspended)and hasnt finished yet
					{
							sprintf(buff,"JobID %d",(jobs_at_start+i+1));//inform the coord
							write(pipe_out,buff,MESSAGESIZE);
					}
					else
					{
						strcpy(buff,"skip");
						write(pipe_out,buff,MESSAGESIZE);
					} 
				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~show_pools~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"show-pools")==0)
			{
				int counter=0;//count how many jobs are active (active means active and suspended?)
				for (i = 0; i <= jobs ; i++)
				{
					if(job_array[i].job_state==1||job_array[i].job_state==2)
					{
						if( waitpid(job_array[i].job_pid,&stat,WNOHANG)!=job_array[i].job_pid)counter++;
					}
				}
				if(counter==0)strcpy(buff,"skip");
				else sprintf(buff,"%d %d",getpid(),counter);
				write(pipe_out,buff,MESSAGESIZE);
			}
		}
		if (termination_signal==0)//if no command was given no termination signal was given the pool updates the finished jobs array
		{
			for (i=0; i < jobs+1; i++)
			{
				if(job_array[i].job_state!=3 && job_array[i].job_state != 0)
				{
					if (waitpid(job_array[i].job_pid,&stat,WNOHANG)==job_array[i].job_pid)
					{ 
						fin++;
						job_array[i].job_state=3;
					}
				}
			}
		}
		if (fin==jobs_pool)//all jobs have finished
		{
			printf("pool ended\n");
			strcpy(buff,"finished");
			write(pipe_out,buff,MESSAGESIZE);
			break;
		}
	}
	close(pipe_in);
	close(pipe_out);
	free(job_array);
	free(timebuff);
	return 1;
}