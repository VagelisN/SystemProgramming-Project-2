#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include "pool_list.h"
#define MESSAGESIZE 70


/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~arg parsing~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
int main(int argc, char **argv)
{
	char c,*inpipe=NULL,*outpipe=NULL,*token=NULL,*path=NULL;
	int jmsout,jmsin,jobs_pool,numofpools=0,numofjobs=0,jobs_acitve_shutdown=0,waiting_answer=0,id,stat,info_given=0,i,waiting_for=0;
	char buff[MESSAGESIZE],tempbuff[MESSAGESIZE],pathbuff[100];
	struct pool_listnode *pool_info=NULL,*last_node=NULL,*temp=NULL;
	pid_t child_id;
	if(argc != 7 && argc != 9){fprintf(stderr, "To run: ./jmscoord -l <path> -n <jobspool> -w <jmsout> -r <jmsin>\n");exit(1);}
	while((c = getopt (argc, argv, "l:n:w:r:")) != -1)
	{
		switch(c)
		{
			case 'l':
				path=malloc(sizeof(char) * (strlen(optarg)+1));
				strcpy(path,optarg);
				mkdir(path,00777);
				break;
			case 'n':
				jobs_pool=atoi(optarg);
				if (jobs_pool <= 0){printf("jobspool needs to be >0 \n");exit(1);}
				break;
			case 'w':
				outpipe=malloc(sizeof(char) * (strlen(optarg)+1));
				strcpy(outpipe,optarg);
				break;
			case 'r':
				inpipe=malloc(sizeof(char) * (strlen(optarg)+1));
				strcpy(inpipe,optarg);
				break;
			default:
				break;
		}		
	}
/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~arg parsing~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/


	if (mkfifo(inpipe , 0666) ==  -1){perror(" mkfifo");exit(1);}//create the fifos to communicate with the console
	if (mkfifo(outpipe , 0666) ==  -1){perror(" mkfifo");exit (1);}
	if ((jmsin=open(inpipe, O_RDONLY|O_NONBLOCK))< 0){perror("fifo  open");exit(1);}//open for read
	if ((jmsout=open(outpipe,O_WRONLY))<0){perror("fifo  open");exit(1);}//wait for the console to open for read (handshake)

	/* the coordinator gets in a loop in which it checks if there is a command from the console. Then, until the answer to that command
	is fully serviced, the coordinator checks ALL the pipes for any possible information. So it doesnt block waiting for an answer from all
	the pools */ 

	/*the waiting_answer flag is used in order to know what types of answers the console is waiting for. when the coordinator
	has sent all the information to the console it is ready to get another command */

	while(1)
	{
		if ( read(jmsin, buff , MESSAGESIZE) > 0)//check if there is a new command from jms_console
		{
			strcpy(tempbuff,buff);
			token=strtok(buff," ");
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~submit~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			if(strcmp(token,"submit")==0)
			{
				if ((numofjobs % jobs_pool)==0)//all previous pools are full
				{
					printf("new pool!\n");
					numofpools++;
					pool_list_insert(&pool_info, &last_node);//insert the newly created pool to the list that keeps information about the pools
					sprintf(pathbuff,"%s%s",path,last_node->in_name);
					if (mkfifo(pathbuff , 0666) ==  -1){perror(" mkfifo");exit(1);}//create the fifos of the newly created pool
					sprintf(pathbuff,"%s%s",path,last_node->out_name);
					if (mkfifo(pathbuff, 0666) ==  -1){perror(" mkfifo");exit(1);}
					if ((last_node->pipe_out=open(pathbuff, O_RDONLY|O_NONBLOCK))< 0){perror("fifo  open");exit(1);}//open the coords reading pipe to start the handshake
					if ((child_id =fork())==0)//in pool process
					{
						pool(last_node->out_name, last_node->in_name,path ,jobs_pool,numofjobs);//Here is the pool process in a function

						/*The pool is forked from the coord so before it exits it needs to clean  all allocated memory*/
						pool_list_delete(pool_info,NULL);
						close(jmsin);
						close(jmsout);
						free(path);
						free(outpipe);
						free(inpipe);
						return 0;	
					}
					else//parent process
					{
						last_node->pool_pid=child_id;
						numofjobs++;
						sprintf(pathbuff,"%s%s",path,last_node->in_name);
						//the pool has opened its side for reading so now the coord can open its writing side and complete the handshake
						if ((last_node->pipe_in=open(pathbuff, O_WRONLY))< 0){perror("fifo  open");exit(1);}
						write(last_node->pipe_in,tempbuff,MESSAGESIZE);
						waiting_answer=1;
					}
				}
				else//last pool is not full so just give it the submit command
				{
					numofjobs++;
					write(last_node->pipe_in,tempbuff,MESSAGESIZE);
					waiting_answer=1;
				}
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~status~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"status")==0)
			{
				id=atoi(strtok(NULL," "));
				if(id <= 0 || id > numofjobs)write(jmsout,"Job id given is not valid",(strlen("Job id given is not valid")+1));
				else
				{
					int temp_id=id,yes=0;
					temp=pool_info;
					while(temp_id >jobs_pool)//find in which pool is the job
					{
						temp=temp->next;
						temp_id-=jobs_pool;
					}
					if(temp->all_finished==1||(waitpid(temp->pool_pid,&stat,WNOHANG)==temp->pool_pid))//if the pool is terminated the job has finished
					{
						sprintf(buff,"JobID %d status: finished",id);
						write(jmsout,buff,MESSAGESIZE);
						yes=1;
					}
					else //ask the pool for its status
					{
						sprintf(buff,"status %d",temp_id);
						write(temp->pipe_in,buff,MESSAGESIZE);
					}
					if(yes==0)waiting_answer=2;	
				}			
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~status_all~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"status-all")==0)
			{
				info_given=0;
				int temp_counter=0,flag=0;
				temp=pool_info;
				while(temp!=NULL)//ask all the pools for the jobs states
				{
					if(temp->all_finished==1||(waitpid(temp->pool_pid,&stat,WNOHANG)==temp->pool_pid))//the pool is terminated so all its jobs are finished
					{
						for (i = 1; i <= jobs_pool ; i++)
						{
							sprintf(buff,"JobID %d status: finished",temp_counter+1);
							temp_counter++;
							write(jmsout,buff,MESSAGESIZE);
							info_given++;
						}
					}
					else //else ask for the status of all the jobs
					{
						flag=1;
						temp_counter+=jobs_pool;
						write(temp->pipe_in,"status-all",(strlen("status-all")+1));
						waiting_answer=5;
					}
					temp=temp->next;
				}
				if(flag==0)write(jmsout,"done",strlen("done")+1);
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~shutdown~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"shutdown")==0)
			{
				// terminates all the pools that havent terminated yet by sending signals.then the program stops  

				temp=pool_info;
				jobs_acitve_shutdown=0;//this is kept in order to know the number of jobs that were still active
				while(temp!=NULL)
				{
					if (temp->all_finished==0)//if the pool is active
					{
						if(read(temp->pipe_out,buff,MESSAGESIZE)>0)//means the pool has finished and hasnt been seen by the coord yet
						{
							if (strcmp(buff,"finished")==0)temp->all_finished=1;
						}
						else
						{
							kill(temp->pool_pid,SIGTERM);//send termination signal
							while(1)
							{
								if(read(temp->pipe_out,buff,MESSAGESIZE)>0)//wait for the message of the pool that has the number of jobs that were active
								{
									jobs_acitve_shutdown+=(atoi(buff));
									break;
								}
							}
						}
					}
					temp=temp->next;
				}
				sprintf(buff,"Served %d jobs, %d were still acitve",numofjobs,jobs_acitve_shutdown);
				jobs_acitve_shutdown=0;
				write(jmsout,buff,MESSAGESIZE);
				break;
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~suspend resume~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"suspend")==0 || strcmp(token,"resume")==0 )
			{
				int sus_or_res=0;
				if(strcmp(token,"suspend")==0)sus_or_res=1;
				id=atoi(strtok(NULL," "));
				if(id <= 0 || id > numofjobs)write(jmsout,"Job id given is not valid",(strlen("Job id given is not valid")+1));
				else
				{
					int temp_id=id;
					temp=pool_info;
					while(temp_id > jobs_pool)//find in which pool is the job
					{
						temp=temp->next;
						temp_id-=jobs_pool;
					}
					if(temp->all_finished==0 && (waitpid(temp->pool_pid,&stat,WNOHANG)!=temp->pool_pid))//if pool is still active send the suspend or resume command
					{
						if(sus_or_res==1)sprintf(buff,"suspend %d",temp_id);
						else sprintf(buff,"resume %d",temp_id);
						write(temp->pipe_in,buff,MESSAGESIZE);
						waiting_answer=1;
					}
					
					else //else no reason to send signal as the job is already finished
					{
						sprintf(buff,"JobID's pool %d has ended",id);
						write(jmsout,buff,MESSAGESIZE);
					}
				}
				
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~show_finished~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"show-finished")==0)
			{
				int flag=0;//this flag is used in order to know if the coord has to wait for any information from the pools
				info_given=0;
				int temp_counter=0;
				temp=pool_info;
				if(temp==NULL)write(jmsout,"done",(strlen("done")+1));//no jobs submited
				while(temp!=NULL)
				{
					if(temp->all_finished==1||(waitpid(temp->pool_pid,&stat,WNOHANG)==temp->pool_pid))//if the pool has finished all its jobs are finished 
					{
						for (i = 1; i <= jobs_pool ; i++)
						{
							sprintf(buff,"JobID %d",temp_counter+1);
							temp_counter++;
							write(jmsout,buff,MESSAGESIZE);
							info_given++;
						}
					}
					else //ask the pool for its finished jobs
					{
						flag=1;
						temp_counter+=jobs_pool;
						write(temp->pipe_in,"show-finished",(strlen("show-finished")+1));
						waiting_answer=3;
					}
					temp=temp->next;
				}
				if(flag==0)write(jmsout,"done",(strlen("done")+1));
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~show_active~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"show-active")==0)
			{
				int flag=0;
				info_given=0;
				temp=pool_info;
				waiting_answer=0;
				if(temp==NULL)write(jmsout,"done",(strlen("done")+1));
				while(temp!=NULL)
				{
					if(temp->all_finished==1 || waitpid(temp->pool_pid,&stat,WNOHANG)==temp->pool_pid)info_given+=jobs_pool;//if the pool has finished it has no active jobs
					else //else ask for the active jobs
					{
						flag=1;
						write(temp->pipe_in,"show-active",(strlen("show-active")+1));
						waiting_answer=3;
					}
					temp=temp->next;
				}
				if(flag==0)write(jmsout,"done",(strlen("done")+1));//since there are no acitve pools theres is nothing to wait for
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~show_pools~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else if(strcmp(token,"show-pools")==0)
			{
				int flag=0;
				waiting_for=0;
				temp=pool_info;
				waiting_answer=0;
				if(temp==NULL)write(jmsout,"done",(strlen("done")+1));
				while(temp!=NULL)
				{
					if(temp->all_finished==0 && waitpid(temp->pool_pid,&stat,WNOHANG)!=temp->pool_pid)//if the pool is active ask if it has active jobs
					{
						flag=1;
						write(temp->pipe_in,"show-pools",(strlen("show-pools")+1));
						waiting_for++;
						waiting_answer=4;
					}
					temp=temp->next;
				}
				if(flag==0)write(jmsout,"done",(strlen("done")+1));
			}
			/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/
			else write(jmsout,"Wrong command given!",(strlen("Wrong command given!")+1));
		}
		else//check if the pools have messages and stay here if waiting for an answer
		{


			//here the coordinator checks the pools in an loop until he has all the answers to the previous command
			while(1)
			{
				temp=pool_info;
				while(temp!=NULL)
				{
					if (temp->all_finished==0)//dont know if all the jobs have finished 
					{
						if(read(temp->pipe_out,buff,MESSAGESIZE)>0)//try to read from its pipe
						{
							if (strcmp(buff,"finished")==0)temp->all_finished=1;//it means the pool is done  
							else
							{
								if (waiting_answer==1)//means submit/suspend/resumed was given
								{
									if(strcmp(buff,"suspended")==0)sprintf(buff,"Sent suspend signal to job %d",id);
									else if(strcmp(buff,"resumed")==0)sprintf(buff,"Sent resume signal to job %d",id);
									write(jmsout,buff,MESSAGESIZE);
									waiting_answer=0;
								}
								else if(waiting_answer==2|| waiting_answer==5)//means status was given or status all
								{
									write(jmsout,buff,MESSAGESIZE);
									if(waiting_answer==5)
									{
										info_given++;
										if(info_given==numofjobs)
										{
											waiting_answer=0;
											write(jmsout,"done",(strlen("done")+1));
										}						
									}
									if(waiting_answer==2)waiting_answer=0;
									
								}
								else if(waiting_answer==3)//means show_active or show_finished was given
								{
									if(strcmp(buff,"skip")==0)info_given++;
									else
									{
										write(jmsout,buff,MESSAGESIZE);
										info_given++;		
									}
									if(info_given==numofjobs)
									{
										info_given=0;
										write(jmsout,"done",(strlen("done")+1));
										waiting_answer=0;
									}
								}
								else if(waiting_answer==4)//means show pools was given
								{
									waiting_for--;
									if(strcmp(buff,"skip")!=0)
									{
										write(jmsout,buff,MESSAGESIZE);
										if(waiting_for==0)
										{
											write(jmsout,"done",(strlen("done")+1));
											waiting_answer=0;
										}
									}
									else if(waiting_for==0)
									{
										write(jmsout,"done",(strlen("done")+1));
										waiting_answer=0;
									}
								}
							}
						}
					}
					temp=temp->next;
				}
				if(waiting_answer==0)break;//all the answers were given to the coord so its ready to take the next command
			}
		}
	}
	pool_list_delete(pool_info,path);
	close(jmsin);
	close(jmsout);
	free(path);
	sprintf(buff,"./%s",inpipe);
	if(unlink(buff)==-1)perror("unlink");
	sprintf(buff,"./%s",outpipe);
	if(unlink(buff)==-1)perror("unlink");
	free(outpipe);
	free(inpipe);
	return 0;
}