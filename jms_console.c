#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#define MESSAGESIZE 70 


int main(int argc, char  **argv)
{
	char c,*inpipe=NULL,*outpipe=NULL,*line=NULL;
	size_t len;
	int jmsout,jmsin,flag=0;
	char buff[MESSAGESIZE];
	FILE *op_file=NULL;
	if(argc != 5 && argc != 7){fprintf(stderr, "To run: ./jmsconsole -w <jmsin> -r <jmsout> -o <operationsfile>\n" );exit(1);}
	while((c = getopt (argc, argv, "w:r:o:")) != -1)
	{
		switch(c)
		{
			case 'w':
				inpipe=malloc(sizeof(char) * (strlen(optarg)+1));
				strcpy(inpipe,optarg);
				break;
			case 'r':
				outpipe=malloc(sizeof(char) * (strlen(optarg)+1));
				strcpy(outpipe,optarg);
				break;
			case 'o':
				if((op_file=fopen(optarg,"r"))==NULL){perror("error opening operations file\n");exit(1);}
				break;
			default:
				break;
		}		
	}
	if(op_file==NULL)op_file=stdin;
	if ((jmsout=open(outpipe, O_RDONLY|O_NONBLOCK))< 0){perror("fifo  open");exit(1);}//first open for read
	jmsin=open(inpipe,O_WRONLY);//when the other side open for read this will unblock and the "handshake" will finish



	while(1)
	{
		if(op_file==stdin)printf("Give a command:\n");
		if(getline(&line, &len,op_file )== -1)//read the op_file line by line. if we reach EOF stdin becomes the input
		{
			fclose(op_file);
			op_file=stdin;
		}
		else 
		{
			if(line[strlen(line)-1]=='\n')line[strlen(line)-1]='\0';
			strcpy(buff,line);
			if(strlen(buff)>1)write(jmsin,buff,MESSAGESIZE);//write the command to the coord
			else flag=1;
			while(1)//wait for the coordinator's answer before sending the the next command
			{
				if(flag==1)
				{
					flag=0;
					break;
				}
				if(strcmp(buff,"show-finished")==0)printf("Finished Jobs:\n");
				if(strcmp(buff,"show-pools")==0)printf("Pools & NumOfJobs:\n");
				if(strcmp(buff,"show-active")==0)printf("Active Jobs:\n");
				if(strcmp(buff,"shutdown")==0)flag=2;

				/*if the command is one of the commands listed below, the console possibly needs more than one answer
				so it waits to get "done" as an answer to stop and give the next command to the coord */
				if(strcmp(buff,"show-finished")==0||strcmp(buff,"show-pools")==0||strcmp(buff,"show-active")==0||strcmp(buff,"status-all")==0)
				{
					while(1)
					{
						if(read(jmsout,buff,MESSAGESIZE) > 0)
						{
							if(strcmp(buff,"done")==0)break;
							printf("%s\n",buff );
						}
					}
					break;
				}
				else if(read(jmsout,buff,MESSAGESIZE) > 0)
				{
					printf("%s\n",buff );
					break;
				}
			}
			if(flag==2)break;
		}
	}
	free(inpipe);
	free(outpipe);
	free(line);
	close(jmsout);
	close(jmsin);
	return 0;
}