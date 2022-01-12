CONFIG_MODULE_SIG=y

module := lsdm
obj-m := $(module).o

KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)
MY_CFLAGS += -g -DDEBUG 
ccflags-y += ${MY_CFLAGS}
CC += ${MY_CFLAGS}

all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules EXTRA_CFLAGS="$(MY_CFLAGS)"
	gcc write_zones.c -o writezones
	gcc read_verify.c -o readverify

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
