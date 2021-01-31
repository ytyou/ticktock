default: build

CC = gcc
CPP = g++
LINK = $(CPP)
RM = /usr/bin/rm -f

#DEBUG_FLAGS = -g3 -rdynamic -ggdb -D_DEBUG -D_TEST -D_GLIBCXX_DEBUG -O0
PROD_FLAGS = -O3
#LEAK_FLAGS = -D_LEAK_DETECTION
#PROF_FLAGS = -pg -D_GLIBCXX_DEBUG
LIBS = /usr/lib64/libz.so

CFLAGS = -I include -std=c++11 $(DEBUG_FLAGS) $(PROF_FLAGS) $(LEAK_FLAGS) $(PROD_FLAGS)
LFLAGS = -lpthread $(PROF_FLAGS) $(DEBUG_FLAGS)

INCS = $(wildcard include/*.h)
TINCS = $(wildcard test/include/*.h)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
TMPS = $(subst src,objs,$(SRCS))
OBJS = $(patsubst %.cpp,%.o,$(TMPS))
OBJI = $(filter-out objs/main.o,$(OBJS))
OBJJ = $(filter-out objs/stress_test.o,$(OBJI))
OBJL = $(filter-out objs/admin.o,$(OBJJ))

SRCT = $(wildcard test/*.cpp)
TMPT = $(subst test,objs,$(SRCT))
OBJT = $(patsubst %.cpp,%.o,$(TMPT))

TARGET = bin/tt
TESTS = bin/all_tests
TOOLS = bin/backfill

build: $(TARGET)

bin/tt: $(OBJS)
	@mkdir -p $(@D)
	$(LINK) $(LFLAGS) -o $@ $(OBJS) $(LIBS)

objs/%.o: src/%.cpp $(INCS)
	@mkdir -p $(@D)
	$(CPP) $(CFLAGS) -c $< -o $@

objs/*/%.o: src/*/%.cpp $(INCS)
	@mkdir -p $(@D)
	$(CPP) $(CFLAGS) -c $< -o $@

objs/tools/%.o: tools/%.cpp
	@mkdir -p $(@D)
	$(CPP) $(CFLAGS) -c $< -o $@

TSRCS = $(wildcard test/*.cpp)
TOBJS = $(patsubst %.cpp,objs/%.o,$(TSRCS))

test: $(TESTS)

objs/test/%.o: test/%.cpp $(INCS) $(TINCS)
	@mkdir -p $(@D)
	$(CPP) $(CFLAGS) -I test/include -c $< -o $@

bin/all_tests: $(OBJI) $(TOBJS)
	@mkdir -p $(@D)
	$(LINK) $(LFLAGS) -o $@ $(OBJI) $(TOBJS) $(LIBS)

tools: $(TOOLS)

bin/backfill: objs/tools/backfill.o
	@mkdir -p $(@D)
	$(LINK) $(LFLAGS) -o $@ $< $(LIBS)

tt: clean build
all: build test tools

clean:
	$(RM) $(OBJS) $(TOBJS) $(TARGET) $(TESTS) $(TOOLS)
