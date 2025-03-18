
version=$1

mkdir -p build/release
git tag -a $version -m "version"
cd ../../build/release && cmake -DCMAKE_BUILD_TYPE=Release  -DUT=OFF -DENABLE_GCOV=OFF -DENABLE_DCC_LITE=ON \
	-DENABLE_MEMCHECK=OFF -DENABLE_EXPORT_API=ON -DSTATISTICS=OFF -DENABLE_BACKUP=OFF \
	-DENABLE_LIBAIO=OFF -DSSL=OFF -DCRYPTO=OFF -DBUILD_TESTS=OFF ../..
cd ../..
git add VERSION.h
git commit -m $version
git tag -f $version -m "version" 
echo "add tag $version success"
#提交commit和tag
#git push 
#git push origin $version   #origin替换成对应远程仓库别名