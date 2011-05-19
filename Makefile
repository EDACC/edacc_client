all:
	mkdir -p bin/verifiers
	$(MAKE) -C src/ all
	$(MAKE) -C verifiers/ all

clean:
	make -C src/ clean
	make -C verifiers/ clean
