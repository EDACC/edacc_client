all:
	make -C src/ all
	make -C verifiers/ all

clean:
	make -C src/ clean
	make -C verifiers/ clean
