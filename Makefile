all: test_assign3_1

test_assign3_1: test_assign3_1.c buffer_mgr.o dberror.o buffer_mgr_stat.o storage_mgr.o record_mgr.o \
				expr.o rm_serializer.o
	gcc -g -Werror -Wall -o test_assign3_1 test_assign3_1.c buffer_mgr.o \
	dberror.o buffer_mgr_stat.o storage_mgr.o record_mgr.o expr.o rm_serializer.o
record_mgr.o: record_mgr.c
	gcc -g -Werror -Wall -c record_mgr.c
expr.o: expr.c
	gcc -g -Werror -Wall -c expr.c
rm_serializer.o: rm_serializer.c
	gcc -g -Werror -Wall -c rm_serializer.c	
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr.o storage_mgr.o dberror.o
	gcc -g -Werror -Wall -c buffer_mgr_stat.c
buffer_mgr.o: buffer_mgr.c storage_mgr.o dberror.o
	gcc -g -Werror -Wall -c buffer_mgr.c
storage_mgr.o: storage_mgr.c dberror.o
	gcc -g -Werror -Wall -c storage_mgr.c
dberror.o: dberror.c
	gcc -g -Werror -Wall -c dberror.c
clean:
	rm *.o test_assign3_1
