.PHONY: all clean

export STATIC=0
export USE_HWLOC=0
# the next two lines are only needed if pkg-config is not available for hwloc and USE_HWLOC = 1
#export HWLOC_INCLUDE=
#export HWLOC_LIB=

all:
	mkdir -p bin/verifiers
	$(MAKE) -C src/ all
	$(MAKE) -C verifiers/ all

clean:
	$(MAKE) -C src/ clean
	$(MAKE) -C verifiers/ clean
