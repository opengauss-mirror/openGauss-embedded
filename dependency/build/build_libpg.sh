
ROOT_DIR=$(cd `dirname $0`/..; pwd)
cd ${ROOT_DIR}/${module_name}/libpg_query
echo $(pwd)

cmake -B build
cmake --build build
