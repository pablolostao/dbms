# Database management system

This repository implements a database management system written in C. Its operation 
is simple if you are familiar with database concepts. It implements:

1. Table creation.
2. Inserts
3. Selects (simple conditions and count)
4. Deletion
5. Updates
6. Buffer pool to keep frequent pages in memory to avoid inefficient disk access.
7. B+Tree index to allow range selection

# Prerequisites:
You must have gcc installed to compile this project
Git
make

# Compilation
Each instance of the program is either a indexer or a peer, so when it is executed, the user must choose.

1.  git clone https://github.com/pablolostao/dbms
2.  cd dbms
2.  make
