import pyspark
from pyspark.sql import SparkSession

lista=[]

#Adicionando os arquivos dos meses 01 a 09 na lista
for i in range(1,10):
  lista.append((
      spark
      .read
      .option('delimiter',';')
      .option('header','true')
      .option('inferSchema','true')
      .option('encoding','UTF-8')
      .csv('precos-gasolina-etanol-'+'0'+str(i)+'.csv')
  ))

#Adicionado o arquivo do mes 10
lista.append(
    (
      spark
      .read
      .option('delimiter',';')
      .option('header','true')
      .option('inferSchema','true')
      .option('encoding','UTF-8')
      .csv('precos-gasolina-etanol-10.csv')
  )


unionDF = lista[0] 
for i in range(1,10):
  unionDF = unionDF.union(lista[i])  #Unindo os dataframes para colocar no mesmo arquivo csv
) 

novo_df = unionDF

novo_df.coalesce(1).write.csv("resultado", header=True, mode="overwrite") #Produzindo o arquivo CSV
