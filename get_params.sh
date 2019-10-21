if [ -z "$1" ]; then
    echo "usage: ./get_params.sh [dataset]"
    exit
fi

rm -rf etc
mkdir etc
mkdir etc/Data
mkdir etc/Test

ls -l "$1" | awk '$1 ~ /d/ {cnt++} END{print cnt}' > etc/params

# 获得每个分类文件个数
for class in "$1"/*; do
if [ -d "$class" ]; then
    class_no_prefix=$(basename $class)
    eval $(ls -l $class | awk '$0 ~ /.txt/ {a++} END {print "cnt="a}')
    if [ $cnt -gt 100 ]; then
        echo ${class_no_prefix} $cnt >> etc/params
        mkdir etc/Test/${class_no_prefix}
        a=0
        for file in $class/*.txt; do
            if [ $a -ge 10 ]; then
                cat $file >> etc/Data/"${class_no_prefix}"
            else
                let a=a+1
                cp $file etc/Test/${class_no_prefix}
            fi
        done
    
    fi
fi
done
