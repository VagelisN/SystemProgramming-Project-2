all: jms_coord jms_console

jms_console: jms_console.o
	gcc -g3 jms_console.o -o jms_console

jms_coord: jms_coord.o pool_list.o
	gcc -g3 jms_coord.o -o jms_coord pool_list.o

jms_console.o: jms_console.c
	gcc -c jms_console.c

jms_coord.o: jms_coord.c 
	gcc -c jms_coord.c

pool_list.o: pool_list.c pool_list.h
	gcc -c pool_list.c	

clean:
	rm *.o jms_coord jms_console
