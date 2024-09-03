

# Get file name with and without extension
filename_ext=$(basename $1)
filename="${filename_ext%.*}"

file_full_path=$1

# Get rest of args
shift
rest_args=$@

g++ $file_full_path -I./include/redis++ -I./include -I./src ./lib/libredis++.a ./lib/libhiredis.a -std=c++17 -o bin/$filename $rest_args    # Using libs compiled in this project