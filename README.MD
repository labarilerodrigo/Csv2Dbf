# Ldbfwrite CLI

```bash
usage: Ldbfwrite [-h] -CSV CSVFILE -DBF DBFFILE -FS FIELDSPECS {} 

positional arguments:
  {impala,beeline}

optional arguments:
  -h, --help            show this help message and exit
  -P PATH, --path PATH  Input folder containing .dbf/.DBF /.zip files to load
  -pr TABPREFIX, --tabprefix TABPREFIX Prefix for generated table. eg. pws_sales_ddd_bricks 
  -E CODEC, --encoding CODEC
                        The codec to use when decoding DBF file
                        records,default is iso-8859-1
```

## + ###
+ 1. Make venv & source bin/activate
+ 2. Copy Ldbfwrite.py and requirements.txt
+ 3. python -m pip install -r requirements.txt
+ 4. python Ldbfwrite.py --path /development/br9/data/brsales/ddd/Inputs/DDDPARAM/ -i usdhdpimpala.rxcorp.com:21050 -d devl_br9 2>&1 | tee test.out


```bash
python Ldbfwrite.py --csv C:\\Users\\RLabarile\\DBF_Files\\NRA_TEST.csv -DBF C:\\Users\\RLabarile\\DBF_Files\\NRA_TEST.DBF -FS "chan_cod C(10); chan_nbr N(20,5)"
```
