build:
	echo "Building the package"
	mkdir build
	cd examples && go build -buildmode=plugin ./*
	cd build && go build ../

	echo "installing the package"
	GOPATH=$(shell pwd)/build/ && go install .

clean:
	echo "Cleaning the package"
	rm -fr build
	find . -type f -iname \*.so -delete

all: build


