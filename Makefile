CC=g++

build: 
	$(CC) -g -o tema1 tema1.cpp -Wall -lpthread

run: tema1
	./tema1

clean:
	rm tema1