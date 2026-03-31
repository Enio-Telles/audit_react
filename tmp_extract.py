import polars as pl
import pprint

df = pl.read_parquet('C:/funcoes - Copia/dados/referencias/referencias/CO_SEFIN/sitafe_produto_sefin_aux.parquet')

with open('C:/audit_react/tmp_schema.txt', 'w') as f:
    f.write(str(df.schema))
    f.write('\n\n')
    f.write(str(df.head()))
