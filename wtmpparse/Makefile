
CFLAGS = -pedantic -Wall

NAME = wtmp2json
.PHONY: all clean

all:
	$(CC) $(CFLAGS) main.c -o $(NAME)

test: all
	python test.py

clean:
	$(RM) $(NAME) *.o
