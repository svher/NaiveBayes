if [ -z "$1" ]; then
    echo "usage: ./train_test_split.sh [dataset]"
    exit
fi

rm -rf etc
mkdir etc
mkdir etc/Data
mkdir etc/Test

for folder in $1/*; do
if [ -d $folder ]; then
    class=$(basename $folder)
    # 获得每个分类文件个数，保存到 numDocuments 变量中
    eval $(ls -l $folder | awk '$0 ~ /.txt/ {a++} END {print "numDocuments="a}')
    if [ $numDocuments -gt 100 ]; then
        mkdir etc/Test/${class}
        let numTest=numDocuments\*3/10
        numSelected=0
        for file in $folder/*.txt; do
          # 按照 7：3 的比例随机选择数据
            if [ $numSelected -gt $numTest -o $RANDOM -le 9830 ]; then
                cat $file >> etc/Data/${class}
            else
                let numSelected=numSelected+1
                cp $file etc/Test/${class}
            fi
        done
        let numTrain=numDocuments-numSelected
        echo ${class} $numTrain >> etc/params
    fi
fi
done
