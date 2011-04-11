CC = g++
CFLAGS=-ggdb -g -W -Wall -Wextra `mysql_config --cflags --libs`
#CFLAGS=-O2 -W -Wall -Wextra `mysql_config --cflags --libs`
COMPILE= $(CC) $(CFLAGS) -c

OBJ_FILES=host_info.o client.o database.o log.o signals.o

all: client

client: $(OBJ_FILES)
	$(CC) $(CFLAGS) $(OBJ_FILES) -o client

client.o: client.cc *.h
	$(COMPILE) client.cc

host_info.o: host_info.cc host_info.h
	$(COMPILE) host_info.cc

database.o: database.cc database.h
	$(COMPILE) database.cc
  
log.o: log.cc log.h
	$(COMPILE) log.cc
	
signals.o: signals.cc signals.h
	$(COMPILE) signals.cc

clean:
	rm -f *.o
	rm -f client

tags: *.cc
	ctags *.cc
