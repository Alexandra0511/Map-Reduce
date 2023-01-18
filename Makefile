CC=g++

build: 
	$(CC) -g -o map-reduce map-reduce.cpp -Wall -lpthread

run: tema1
	./map-reduce

clean:
	rm map-reduce
