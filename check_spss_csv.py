import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


df = pd.read_csv('/home/osboxes/Scrivania/ANAGRAFE_AIC_TOTALE_2021-07-01.CSV', sep=';')
df.to_csv('/home/osboxes/Scrivania/pandas_export_ANAGRAFE_AIC_TOTALE_2021-07-01.CSV', sep=';', header=True,
          index=False)


print(df)