if [ -z "$1" ]; then
echo "usage: ./get_params.sh [dataset]"
exit
fi

if [ ! -d etc ]; then
mkdir etc
fi

if [ -d etc/Data ]; then
rm -rf etc/Data
fi

mkdir etc/Data

ls -l "$1" | awk '$1 ~ /d/ {cnt++} END{print cnt}' > etc/params

# 获得每个分类文件个数
for class in "$1"/*
do
if [ -d "$class" ]; then
class_no_prefix=$(basename "$class")
cat "$class"/* > etc/Data/"${class_no_prefix}"
ls -l "$class" | awk -v class="$class_no_prefix" '$0 ~ /.txt/ {a++} END {print class,a}' >> etc/params
fi
done