g++ -O3 src/search.cc -S -march=native 
g++ -c search.s 
mv search.o lib/
rm -rf build
mkdir build