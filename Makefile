all: test_assign2_1

test_assign2_1: test_assign2_1.c buffer_mgr.o dberror.o buffer_mgr_stat.o storage_mgr.o
	gcc -g -Werror -Wall -o test_assign2_1 test_assign2_1.c buffer_mgr.o dberror.o buffer_mgr_stat.o storage_mgr.o
test_assign2_2: test_assign2_2.c buffer_mgr.o dberror.o buffer_mgr_stat.o storage_mgr.o
	gcc -g -Werror -Wall -o test_assign2_2 test_assign2_2.c buffer_mgr.o dberror.o buffer_mgr_stat.o storage_mgr.o
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr.o storage_mgr.o dberror.o
	gcc -g -Werror -Wall -c buffer_mgr_stat.c
buffer_mgr.o: buffer_mgr.c storage_mgr.o dberror.o
	gcc -g -Werror -Wall -c buffer_mgr.c
storage_mgr.o: storage_mgr.c dberror.o
	gcc -g -Werror -Wall -c storage_mgr.c
dberror.o: dberror.c
	gcc -g -Werror -Wall -c dberror.c
clean:
	rm *.o test_assign2_1
	rm *.o test_assign2_2
