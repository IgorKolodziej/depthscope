CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror
PYTHON ?= python3

.PHONY: all test docs
all: build/libdepthscope.so

build/libdepthscope.so: depthscope/book.c
	mkdir -p build
	$(CC) $(CFLAGS) -std=c17 -fPIC -shared $< -o $@ -lz

test: all
	$(PYTHON) -m unittest discover -s tests -v

docs:
	$(MAKE) -C milestones/data pdf
