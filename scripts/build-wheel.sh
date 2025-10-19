#!/bin/bash

rm -rf dist 
python -m build --wheel
temp_dir=$(mktemp -d)
unzip dist/*.whl -d $temp_dir
for so_file in libdftracer_utils.so.0 libxxhash.so; do
    so=$(find $temp_dir -name $so_file)
    path=`dirname $so`
    path=$(realpath $(dirname $path))
    echo $path
    export LD_LIBRARY_PATH=$path/lib:$path/lib64:$LD_LIBRARY_PATH
done
echo
echo "LD LIBRARY PATH: $LD_LIBRARY_PATH"
echo
auditwheel show dist/*.whl
platform=$(auditwheel show dist/*.whl | grep manylinux_ | awk -F'"' '{print $2}')
auditwheel show dist/*.whl | grep manylinux_
echo "Platform selected is $platform"
auditwheel repair --plat $platform dist/*.whl -w dist/
rm -rf $temp_dir
rm dist/*linux_x86_64.whl
rm dist/*.tar.gz
