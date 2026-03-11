# 预处理数据
if [ ! -d "_benchmarks/CResQA" ]; then
    mkdir -p _benchmarks/CResQA
fi
# rm -r _benchmarks/CResQA/*

# 处理数据集
# cp ../../dataset/CResQA/* _benchmarks/CResQA # 不能直接使用，需要进行字段转换
python prepare_cresqa.py

ls -lh _benchmarks/
ls -lh _benchmarks/CResQA