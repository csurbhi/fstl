CONFIG_MODULE_SIG=n

module := lsdm
obj-m := $(module).o

KDIR := /lib/modules/$(shell uname -r)/build
#KDIR := /lib/modules/4.10.0-42-generic/build


PWD := $(shell pwd)
CFLAGS_dm-$(module).o += -DDEBUG -g

all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
