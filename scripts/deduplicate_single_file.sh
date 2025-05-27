set -e
python3 scripts/make_suffix_array.py $1
cargo run self-similar --data-file $1 --length-threshold $3 --cache-dir /kaggle/working/tmp --num-threads $4
cargo run collect --data-file $1 --cache-dir /kaggle/working/tmp --length-threshold $3 > /kaggle/working/tmp/drop_tokens_file
python3 scripts/finish_single_file.py $1 /kaggle/working/tmp/drop_tokens_file $2
