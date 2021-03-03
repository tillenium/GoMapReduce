# Map Reduce in Go

### Installation
```shell
make clean && make build
```

### Running instructions
```shell
./build/bin/gomr controller  <files>
./build/bin/gomr worker <.so file with Map/Reduce operation>

#example:

./build/gomr controller data/file1.txt data/file2.txt
./build/gomr worker ./examples/word_count.so      
```

### Design Docs
```shell
Design Docs are under ./docs folder.
```